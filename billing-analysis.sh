#!/bin/bash
DATASET='billing_analysis'
BUCKET='gs://gce_billing_data'
MONTH=${1:-$(date +%Y-%m)}
shift

get_create_pie_chart() {
	if [[ ! -e /tmp/create-pie-chart ]]; then
		curl -so /tmp/create-pie-chart 'https://raw.githubusercontent.com/justin8/scripts/master/create-pie-chart'
		chmod +x /tmp/create-pie-chart
	fi
}

create_dataset() {
	if ! bq ls | grep -q $DATASET; then
		bq mk $DATASET
	fi
}

create_schema() {
	# Thanks google for specifying "This feature is in preview. The formatting and content of these files might change."
	# The schema appears to changes depending on what day of the month it is or something equally crazy. Let's figure it out on the fly
	local type
	local fields
	local schema
	local field
	local lastpos
	local lastfield
	local eof=,
	local temp="$(mktemp)"

	gsutil cp "$1" "$temp" &>/dev/null

	schema='['
	IFS=, read -r -a fields <<< "$(head -n1 "$temp" | tr '[:upper:]' '[:lower:]' | tr ' ' '_')"
	lastpos=$(( ${#fields[*]} - 1 ))
	lastfield="${fields[$lastpos]}"
	for field in "${fields[@]}"; do
		[[ $field == "$lastfield" ]] && eof=''
		case $field in
			start_time) type=timestamp;;
			end_time) type=timestamp;;
			project) type=integer;;
			measurement1_total_consumption) type=float;;
			credit_amount) type=float;;
			cost) type=float;;
			project_number) type=integer;;
			*) type=string;;
		esac
		schema+=$(cat <<-EOF
		{
		  "name": "$field",
		  "type": "$type",
		  "mode": "NULLABLE"
		}$eof
		EOF
		)
	done
	schema+=']'

	rm "$temp"
	echo "$schema"
}

import_data() {
	local TEMPDIR=$(mktemp -d)
	local INPUT_DATA=$(gsutil ls $BUCKET | grep "$MONTH")

	[[ ! $INPUT_DATA ]] && echo "No input data found!" && exit 1

	if bq ls $DATASET | grep -q ${MONTH/-/_}; then
		echo "The ${MONTH/-/_} table already exists in the dataset $DATASET."
		read -p "Do you wish to delete and rebuild it? (This could take 5+ minutes) [y/N]" response
		[[ ! $response =~ [Yy].* ]] && return 0
	fi

	bq rm -f "$DATASET.${MONTH/-/_}"

	echo "Importing all data for the month $MONTH (this could take 5+ minutes)..."
	for CSV in $INPUT_DATA; do
		(
			for i in {1..3}; do
				create_schema "$CSV" > "$TEMPDIR/${CSV##*/}"
				err=$(bq load --skip_leading_rows=1 "$DATASET.${MONTH/-/_}" "$CSV" "$TEMPDIR/${CSV##*/}" 2>&1)
				rc=$?
				[[ $rc -eq 0 ]] && break
			done

			if [[ $rc -eq 0 ]]; then
				echo "Success: $CSV"
			else
				echo "Failure: $CSV"
				echo "$err"
			fi
		) &
	done

	wait
	rm -rf "$TEMPDIR"
}

query_data() {
	local temp="$(mktemp)"

	echo "This data is located in $temp" > "$temp"
	cat <<-EOF | bq query --max_rows=100000000 >> $temp
	SELECT
	  measurement1,
	  sum(measurement1_total_consumption) as measurement1_total_consumption,
	  measurement1_units,
	  sum(cost) as cost
	FROM
	  billing_analysis.${MONTH/-/_}
	GROUP BY
	  measurement1,
	  measurement1_units
	ORDER BY
	  cost DESC
	EOF

	echo "$temp"
}

create_pie_data() {
	local data=$1
	local keywords=$2

	while read entry; do
		if [[ $keywords ]]; then
			if ! echo "$entry" | grep -qP "$keywords"; then
				continue
			fi
		fi
		key=$(echo "$entry" | cut -d'|' -f2 | tr -d '[:space:]')
		value=$(echo "$entry" | cut -d'|' -f5 | tr -d '[:space:]')
		echo -n " ${key#*services/}:$value"
	done <<<"$(head -n-1 "$data" | tail -n+6)"
}

display_pie() {
	local data=$1
	local keywords=$2
	local pie_data

	get_create_pie_chart

	pie_data=$(create_pie_data "$data" "$keywords")

	bash /tmp/create-pie-chart "Billing analysis for $MONTH" $pie_data
}

display_table() {
	local data=$1

	less -S "$data"
}

create_dataset
import_data
data=$(query_data)

case $1 in
	--pie) display_pie "$data" "$2" ;;
	*) display_table "$data" ;;
esac
