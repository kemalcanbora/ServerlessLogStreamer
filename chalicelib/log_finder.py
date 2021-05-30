from botocore.errorfactory import ClientError


class LogFinder:
    def __init__(self, session):
        self.client = session.client('logs')

    def finder(self, lambda_name):
        log_name = "/aws/lambda/{}".format(lambda_name)
        try:
            stream_response = self.client.describe_log_streams(
                logGroupName=log_name,
                orderBy='LastEventTime',
                limit=1,
                descending=True
            )

            latestlogStreamName = stream_response["logStreams"][0]["logStreamName"]
            response = self.client.get_log_events(logGroupName=log_name,
                                                  logStreamName=latestlogStreamName)

            return response

        except ClientError as ce:
            if ce.response['Error']['Code'] == 'ResourceNotFoundException':
                return {"message": "The specified log group does not exist"}
