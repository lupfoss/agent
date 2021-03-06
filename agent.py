import http.client
import json
import time
import sys
from typing import Dict, List, Optional
import logging
import sqlalchemy
import config

MOTHERSHIP = config.MOTHERSHIP
DB_PARAMS = config.DB_PARAMS

SQL_ERROR = "Unable to complete SQL: %.1000s %s %s"

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("agent.log"),
        logging.StreamHandler()
    ]
)
#logger.addHandler(logging.StreamHandler())
#logger.setLevel(logging.DEBUG)

class AgentDelegate:

    # Need config like the following:
    # db_params = {
    #     "type": "ATHENA",
    #     "region_name": "us-west-2",
    #     "aws_access_key_id": "xxx",
    #     "aws_secret_access_key": "yyy",
    #     "s3_staging_dir": "s3://<bucket>/<folder>",
    #     "dbname": "<dbname>",
    # }
    def __init__(self, db_params):
        if db_params['type'] != 'ATHENA':
            logger.error("data source type not implemented")
            sys.exit(1)

        protocol = "awsathena+rest"
        host = "athena.{region_name}.amazonaws.com:443".format(
                        region_name=db_params["region_name"]
                )

        url = (
                    protocol
                    + "://{aws_access_key_id}:{aws_secret_access_key}@{host}/{schema_name}?s3_staging_dir={s3_staging_dir}".format(
                        host=host,
                        aws_access_key_id=db_params["aws_access_key_id"],
                        aws_secret_access_key=db_params["aws_secret_access_key"],
                        schema_name=db_params["dbname"],
                        s3_staging_dir=db_params["s3_staging_dir"],
                    )
                )


        self.engine = sqlalchemy.create_engine(url)

    def fetchall(
        self, query, params: Optional[Dict] = None, prescript: Optional[str] = None
    ):
        engine = self.engine
        with engine.connect() as conn:
            if prescript:
                conn.execute(prescript)
            result = None
            try:
                if params:
                    result = conn.execute(query, params)
                else:
                    result = conn.execute(query)
                return result.fetchall()
            except Exception as ex:
                logger.error(SQL_ERROR, query, params, str(ex))
                raise
            finally:
                if result:
                    result.close()

    def fetchall_dict(
        self, query: str, params: Optional[Dict] = None, prescript: Optional[str] = None
    ) -> List[Dict]:
        result = self.fetchall(query, params=params, prescript=prescript)
        return [dict(data) for data in result]


class Agent:
    def __init__(self):
        self.delegate = AgentDelegate(DB_PARAMS)

    def get_next_command(self):
        # get
        connection = http.client.HTTPConnection(MOTHERSHIP)

        connection.request("GET", "/command")
        response = connection.getresponse()
        logger.info("Status: {} and reason: {}".format(response.status, response.reason))
        text = response.read().decode('utf-8')
        logger.info(text)
        j = json.loads(text)
        qid = j['uuid']
        dbquery = j['dbquery']
        logger.info(qid)
        logger.info(dbquery)

        self.qid = qid
        self.dbquery = dbquery

        connection.close()

        return response

    def run_command(self, command):
        logger.info("running command locally")
        logger.info(self.qid)
        logger.info(self.dbquery)
        if self.dbquery:
            try:
                result = self.delegate.fetchall_dict(self.dbquery)
            except Exception as e:
                result = str(e)
            return result
        return None

    def post_command_result(self, result):
        if not result:
            return

        conn = http.client.HTTPConnection(MOTHERSHIP)

        headers = {'Content-type': 'application/json'}

        data = {'uuid': self.qid, 'result': result}
        json_data = json.dumps(data)

        conn.request('POST', '/post', json_data, headers)

        response = conn.getresponse()
        logger.info(response.read().decode())

    def do_iteration(self):
        logger.info("asking for command")
        command = self.get_next_command()

        logger.info("running command")
        result = self.run_command(command)
        logger.info(result)

        logger.info("asking for command")
        self.post_command_result(result)


def main():
    run_duration = 10
    if len(sys.argv) > 1:
      try:
        run_duration = int(sys.argv[1])
      except Exception as e:
        logger.error('exception parsing run duration, will use default')

    exit_by = time.time() + run_duration

    agent = Agent()

    while True:
        try:
            agent.do_iteration()

        except Exception as e:
            logger.error(e)

        time.sleep(2)

        if time.time() > exit_by:
            break

    logger.info('exiting agent')


if __name__ == "__main__":
    main()
