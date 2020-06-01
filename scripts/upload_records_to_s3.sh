#!/usr/bin/env bash

REMOTE_CSV_DIR="s3://mhtang/data-learning-test/data/csvs"

list_of_zip_files=(
  "20200524054500.mentions.CSV.zip"
  #  "20200524060000.mentions.CSV.zip"
  #  "20200524061500.mentions.CSV.zip"
  #  "20200524063000.mentions.CSV.zip"
  #  "20200524064500.mentions.CSV.zip"
  #  "20200524070000.mentions.CSV.zip"
  #  "20200524071500.mentions.CSV.zip"
  #  "20200524073000.mentions.CSV.zip"
  #  "20200524074500.mentions.CSV.zip"
  #  "20200524080000.mentions.CSV.zip"
  #  "20200524081500.mentions.CSV.zip"
  #  "20200524083000.mentions.CSV.zip"
  #  "20200524084500.mentions.CSV.zip"
  #  "20200524090000.mentions.CSV.zip"
  #  "20200524091500.mentions.CSV.zip"
  #  "20200524093000.mentions.CSV.zip"
  #  "20200524094500.mentions.CSV.zip"
  #  "20200524100000.mentions.CSV.zip"
  #  "20200524101500.mentions.CSV.zip"
  #  "20200524103000.mentions.CSV.zip"
  #  "20200524104500.mentions.CSV.zip"
  #  "20200524110000.mentions.CSV.zip"
  #  "20200524111500.mentions.CSV.zip"
  #  "20200524113000.mentions.CSV.zip"
  #  "20200524114500.mentions.CSV.zip"
  #  "20200524120000.mentions.CSV.zip"
  #  "20200524121500.mentions.CSV.zip"
  #  "20200524123000.mentions.CSV.zip"
  #  "20200524124500.mentions.CSV.zip"
  #  "20200524130000.mentions.CSV.zip"
  #  "20200524131500.mentions.CSV.zip"
  #  "20200524133000.mentions.CSV.zip"
  #  "20200524134500.mentions.CSV.zip"
  #  "20200524140000.mentions.CSV.zip"
  #  "20200524141500.mentions.CSV.zip"
  #  "20200524143000.mentions.CSV.zip"
  #  "20200524144500.mentions.CSV.zip"
  #  "20200524150000.mentions.CSV.zip"
  #  "20200524151500.mentions.CSV.zip"
  #  "20200524153000.mentions.CSV.zip"
  #  "20200524154500.mentions.CSV.zip"
  #  "20200524160000.mentions.CSV.zip"
  #  "20200524161500.mentions.CSV.zip"
  #  "20200524163000.mentions.CSV.zip"
  #  "20200524164500.mentions.CSV.zip"
  #  "20200524170000.mentions.CSV.zip"
  #  "20200524171500.mentions.CSV.zip"
  #  "20200524173000.mentions.CSV.zip"
  #  "20200524174500.mentions.CSV.zip"
  #  "20200524180000.mentions.CSV.zip"
  #  "20200524181500.mentions.CSV.zip"
  #  "20200524183000.mentions.CSV.zip"
  #  "20200524184500.mentions.CSV.zip"
  #  "20200524190000.mentions.CSV.zip"
  #  "20200524191500.mentions.CSV.zip"
  #  "20200524193000.mentions.CSV.zip"
  #  "20200524194500.mentions.CSV.zip"
  #  "20200524200000.mentions.CSV.zip"
  #  "20200524201500.mentions.CSV.zip"
  #  "20200524203000.mentions.CSV.zip"
  #  "20200524204500.mentions.CSV.zip"
  #  "20200524210000.mentions.CSV.zip"
  #  "20200524211500.mentions.CSV.zip"
  #  "20200524213000.mentions.CSV.zip"
  #  "20200524214500.mentions.CSV.zip"
  #  "20200524220000.mentions.CSV.zip"
  #  "20200524221500.mentions.CSV.zip"
  #  "20200524223000.mentions.CSV.zip"
  #  "20200524224500.mentions.CSV.zip"
  #  "20200524230000.mentions.CSV.zip"
  #  "20200524231500.mentions.CSV.zip"
  #  "20200524233000.mentions.CSV.zip"
  #  "20200524234500.mentions.CSV.zip"
)

for file in "${list_of_zip_files[@]}"; do
  LOCAL_ZIP_FILE_PATH="./data/$file"
  rm -f "$LOCAL_ZIP_FILE_PATH"

  echo "Downloading and uploading $file"
  curl -o "$LOCAL_ZIP_FILE_PATH" "http://data.gdeltproject.org/gdeltv2/$file"

  CSV_FILE_NAME="${file/.zip/}"
  REMOTE_CSV_FILE_PATH="$REMOTE_CSV_DIR/$CSV_FILE_NAME"
  LOCAL_CSV_FILE_PATH="./data/$CSV_FILE_NAME"

  rm -f "$LOCAL_CSV_FILE_PATH"
  unzip -o "$LOCAL_ZIP_FILE_PATH" -d "./data"
  rm -f "$LOCAL_ZIP_FILE_PATH"

  aws --profile profile_tw s3 rm "$REMOTE_CSV_FILE_PATH"
  aws --profile profile_tw s3 cp "$LOCAL_CSV_FILE_PATH" "$REMOTE_CSV_FILE_PATH"
done

echo "Running EMR job"
./scripts/run_on_emr.sh "$REMOTE_CSV_FILE_PATH"
