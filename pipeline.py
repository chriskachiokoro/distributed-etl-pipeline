import argparse
import csv
import io
import json
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

# Expected Reddit CSV schema (order must match your files)
EXPECTED_COLUMNS = [
    "id",
    "subreddit",
    "created_utc",
    "title",
    "selftext",
    "score",
    "num_comments",
    "upvote_ratio",
    "title_and_text",
    "url",
    "Season"
]

# Extra columns for enriched data
VALID_COLUMNS = EXPECTED_COLUMNS + ["created_time", "created_hour"]
FILTERED_COLUMNS = VALID_COLUMNS
INVALID_COLUMNS = EXPECTED_COLUMNS + ["error_messages"]


def parse_csv_line(line):
    """Parse a CSV line into a dict using EXPECTED_COLUMNS."""
    # Skip header line by checking first column
    if line.startswith("id,subreddit,created_utc"):
        return None

    values = next(csv.reader([line]))
    if len(values) != len(EXPECTED_COLUMNS):
        # malformed row
        return {"__malformed__": True, "raw_line": line}

    return dict(zip(EXPECTED_COLUMNS, values))


def dict_to_csv_line(row, columns):
    """Convert a dict to a CSV line with the given column order."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([row.get(col, "") for col in columns])
    return output.getvalue().strip("\r\n")


class ValidateAndClassify(beam.DoFn):
    """
    Validate each row and classify into:
    - invalid (schema/type errors)
    - valid (meets business rules)
    - filtered (good structure but fails business rules)
    """

    def process(self, row):
        # Handle malformed rows from parse step
        if row is None:
            return

        if row.get("__malformed__"):
            row_copy = {"error_messages": "Malformed CSV row", "raw_line": row["raw_line"]}
            yield beam.pvalue.TaggedOutput("invalid", row_copy)
            return

        errors = []

        # Required columns present and non-empty
        required = [
            "id",
            "subreddit",
            "created_utc",
            "title",
            "score",
            "num_comments",
            "upvote_ratio",
            "Season",
        ]
        for col in required:
            if row.get(col) in ("", None):
                errors.append(f"Missing value in {col}")

        # Type checks and conversions
        created_dt = None
        try:
            created_ts = int(row["created_utc"])
            created_dt = datetime.utcfromtimestamp(created_ts)
        except Exception:
            errors.append("created_utc must be valid integer timestamp")

        try:
            score = int(row["score"])
        except Exception:
            errors.append("score must be int")
            score = None

        try:
            num_comments = int(row["num_comments"])
        except Exception:
            errors.append("num_comments must be int")
            num_comments = None

        try:
            ratio = float(row["upvote_ratio"])
            if not (0.0 <= ratio <= 1.0):
                errors.append("upvote_ratio must be between 0 and 1")
        except Exception:
            errors.append("upvote_ratio must be float")
            ratio = None

        # If any structural/type errors → invalid
        if errors or created_dt is None or num_comments is None:
            invalid_row = {col: row.get(col, "") for col in EXPECTED_COLUMNS}
            invalid_row["error_messages"] = "; ".join(errors)
            yield beam.pvalue.TaggedOutput("invalid", invalid_row)
            return

        # Enrich with created_time and created_hour
        enriched = dict(row)
        enriched["created_time"] = created_dt.isoformat()
        enriched["created_hour"] = str(created_dt.hour)  # store as string for CSV

        # Business rule: afternoon (12–17) AND >5 comments
        business_ok = 12 <= created_dt.hour <= 17 and num_comments > 5

        if business_ok:
            # Structurally valid + meets business rule
            yield beam.pvalue.TaggedOutput("valid", enriched)
        else:
            # Structurally valid, but does NOT meet business rules
            yield beam.pvalue.TaggedOutput("filtered", enriched)


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True, help="GCS path to input CSV")
    parser.add_argument("--output_clean", required=True, help="GCS prefix for valid output CSV")
    parser.add_argument("--output_quarantine", required=True, help="GCS prefix for invalid rows CSV")
    parser.add_argument("--output_filtered", required=True, help="GCS prefix for filtered-out rows CSV")
    parser.add_argument("--log_output", required=True, help="GCS prefix for JSON logs of invalid rows")

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Make sure the pipeline can be pickled (good practice)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        # Read lines from input CSV
        lines = p | "ReadCSV" >> beam.io.ReadFromText(known_args.input)

        # Parse lines into dicts
        parsed = (
            lines
            | "ParseCSV" >> beam.Map(parse_csv_line)
            | "DropNone" >> beam.Filter(lambda r: r is not None)
        )

        # Validate & classify
        classified = (
            parsed
            | "ValidateAndClassify" >> beam.ParDo(ValidateAndClassify())
              .with_outputs("valid", "invalid", "filtered")
        )

        valid_rows = classified.valid
        invalid_rows = classified.invalid
        filtered_rows = classified.filtered

        # ---- Write valid (clean) rows as CSV ----
        (
            valid_rows
            | "ValidToCSV" >> beam.Map(lambda row: dict_to_csv_line(row, VALID_COLUMNS))
            | "WriteValid" >> beam.io.WriteToText(
                known_args.output_clean,
                file_name_suffix=".csv",
                shard_name_template="-SSSSS-of-NNNNN",
                header=",".join(VALID_COLUMNS),
            )
        )

        # ---- Write filtered-out rows as CSV ----
        (
            filtered_rows
            | "FilteredToCSV" >> beam.Map(lambda row: dict_to_csv_line(row, FILTERED_COLUMNS))
            | "WriteFiltered" >> beam.io.WriteToText(
                known_args.output_filtered,
                file_name_suffix=".csv",
                shard_name_template="-SSSSS-of-NNNNN",
                header=",".join(FILTERED_COLUMNS),
            )
        )

        # ---- Write invalid (quarantine) rows as CSV ----
        (
            invalid_rows
            | "InvalidToCSV" >> beam.Map(lambda row: dict_to_csv_line(row, INVALID_COLUMNS))
            | "WriteInvalid" >> beam.io.WriteToText(
                known_args.output_quarantine,
                file_name_suffix=".csv",
                shard_name_template="-SSSSS-of-NNNNN",
                header=",".join(INVALID_COLUMNS),
            )
        )

        # ---- Also write JSON logs for invalid rows ----
        (
            invalid_rows
            | "InvalidToJSON" >> beam.Map(lambda row: json.dumps(row))
            | "WriteLogs" >> beam.io.WriteToText(
                known_args.log_output,
                file_name_suffix=".json",
                shard_name_template="-SSSSS-of-NNNNN",
            )
        )


if __name__ == "__main__":
    run()