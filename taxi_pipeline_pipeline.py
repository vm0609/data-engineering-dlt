import dlt
import requests
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@dlt.source(name="nyc_taxi_data")
def nyc_taxi_data_source():
    """
    A dlt source for the NYC taxi data API.
    """
    base_url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

    @dlt.resource(
        write_disposition="replace"
    )
    def taxi_trips():
        """
        A dlt resource for the taxi trips data.
        """
        page = 1
        total_records = 0
        while True:
            try:
                params = {"page": page}
                logging.info(f"Fetching page {page} from {base_url} with params {params}")
                response = requests.get(base_url, params=params)
                response.raise_for_status()
                data = response.json()

                if not data:
                    logging.info("API returned an empty list. Stopping pagination.")
                    break

                record_count = len(data)
                total_records += record_count
                logging.info(f"Page {page} returned {record_count} records. Total records so far: {total_records}")

                # Log the first record of the first page to inspect the structure
                if page == 1 and record_count > 0:
                    logging.info(f"First record structure: {data[0]}")

                yield data
                page += 1

            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching data from API: {e}")
                break
            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")
                break
        
        logging.info(f"Finished fetching data. Total records yielded: {total_records}")

    return taxi_trips

def pipeline():
    """
    This function creates and runs a dlt pipeline to load NYC taxi data into DuckDB.
    """
    logging.info("Starting dlt pipeline execution.")
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="ny_taxi_data",
    )
    load_info = pipeline.run(nyc_taxi_data_source())
    logging.info("dlt pipeline execution finished.")
    print(load_info)

if __name__ == "__main__":
    pipeline()