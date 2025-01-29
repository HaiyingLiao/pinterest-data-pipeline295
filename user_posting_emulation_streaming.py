import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml


random.seed(100)

class AWSDBConnector:

    def __init__(self):
        creds = self.read_db_creds("db_creds.yaml")
        self.HOST = creds["HOST"]
        self.USER = creds["USER"]
        self.PASSWORD = creds["PASSWORD"]
        self.DATABASE = creds["DATABASE"]
        self.PORT = creds["PORT"]
    
    def read_db_creds(self, yaml_file):
        with open(yaml_file,"r") as creds_file:
          creds = yaml.safe_load(creds_file)
        return creds
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                invoke_url = "https://udfk1yk5bj.execute-api.us-east-1.amazonaws.com/dev/streams/Kinesis-Prod-Stream/record"

                payload = json.dumps({
                    "StreamName": "Kinesis-Prod-Stream",
                    "Data": {"index": pin_result["index"], "unique_id": pin_result["unique_id"], "title": pin_result["title"],"description": pin_result["description"],"poster_name": pin_result["poster_name"],"follower_count": pin_result["follower_count"],"tag_list": pin_result["tag_list"],"is_image_or_video": pin_result["is_image_or_video"],"image_src": pin_result["image_src"],"downloaded": pin_result["downloaded"],"save_location": pin_result["save_location"], "category": pin_result["category"] },
                    "PartitionKey": "df_pin"
                                      })
                headers = {'Content-Type': 'application/json'}
                response = requests.request("PUT", invoke_url, headers=headers, data=payload)
                print(response.status_code)
                print("pin_result: ", response.content)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                # Convert datetime object to string
                geo_result["timestamp"] = geo_result["timestamp"].isoformat() 

                invoke_url = "https://udfk1yk5bj.execute-api.us-east-1.amazonaws.com/dev/streams/Kinesis-Prod-Stream/record"
                payload = json.dumps({
                    "StreamName": "Kinesis-Prod-Stream",
                    "Data": {"ind": geo_result["ind"], "timestamp": geo_result["timestamp"], "latitude": geo_result["latitude"],"longitude": geo_result["longitude"],"country": geo_result["country"] },
                    "PartitionKey": "df_geo"
                                      })
                headers = {'Content-Type': 'application/json'}
                response = requests.request("PUT", invoke_url, headers=headers, data=payload)
                print(response.status_code)
                print("geo_result: ", response.content)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                # Convert datetime object to string
                user_result["date_joined"] = user_result["date_joined"].isoformat() 

                invoke_url = "https://udfk1yk5bj.execute-api.us-east-1.amazonaws.com/dev/streams/Kinesis-Prod-Stream/record"
                payload = json.dumps({
                    "StreamName": "Kinesis-Prod-Stream",
                    "Data": {"ind": user_result["ind"], "first_name": user_result["first_name"], "last_name": user_result["last_name"],"age": user_result["age"],"date_joined": user_result["date_joined"] },
                    "PartitionKey": "df_user"
                                      })
                headers = {'Content-Type': 'application/json'}
                response = requests.request("PUT", invoke_url, headers=headers, data=payload)
                print(response.status_code)
                print("user_result: ",response.content)



if __name__ == "__main__":
    run_infinite_post_data_loop()
    
    
    


