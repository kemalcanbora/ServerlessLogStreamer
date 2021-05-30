from chalicelib.redis_connection import Redis, SubPub
from chalicelib.log_finder import LogFinder
from chalicelib.helper import Pool
from chalice import Chalice, Rate
from multiprocessing import cpu_count
import boto3
import ast
import os

app = Chalice(app_name='ServerlessLogStreamer')
redis_client = Redis().connection()

session = boto3.Session(aws_access_key_id=os.environ["aws_access_key_id"],
                        aws_secret_access_key=os.environ["aws_secret_access_key"],
                        region_name=os.environ["aws_region_name"])



def redis_schedule_lambda(lambda_name):
    result = LogFinder(session=session).finder(lambda_name=lambda_name)
    for log in result["events"]:
        SubPub().publisher(channel=lambda_name, message=log)
    return {'redis_publisher': lambda_name}


@app.route("/{lambda_name}", methods=["POST", "GET"])
def log_function(lambda_name):
    result = LogFinder(session=session).finder(lambda_name=lambda_name)
    return {"log": result}


@app.route("/search", methods=["POST", "GET"])
def search_lambda_log():
    text = app.current_request.query_params.get('text', None)
    result = [item.__dict__ for item in Redis().search_value(client=redis_client, search_text=text).docs]
    return {"log": result}


@app.schedule(Rate(1, unit=Rate.MINUTES))
def schedule_lambda(event):
    pool = Pool(process_count=cpu_count())
    lambda_list = ast.literal_eval(os.environ["lambda_list"])
    pool.map(redis_schedule_lambda, lambda_list)
    return {"status": True}
