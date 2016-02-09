import json
from flask import Flask, request, Response, jsonify

from exception.smt_exception import SMTException
from runner.service_runner import ServiceRunner
from log.smt_logger import SMTLogger
from util.smt_constants import Constants

app = Flask(__name__, instance_relative_config=True)
app.config.from_pyfile('service-stag.py')

# logger object
logger = SMTLogger.get_logger()


@app.route('/status', methods=['GET'])
def status_check():
    """
    Status check, to see if service is up
    :return:
    """
    return 'SUCCESS'


@app.route('/', methods=['POST'])
def hello():
    """
    Main handler for servicing translation requests
    :return:
    """
    logger.info(request.json)
    exit_code = Constants.GRACE_EXIT_CODE
    str_error = "Translation completed successfully!"
    try:
        smt = ServiceRunner.from_json(request, app.config)
        out = smt.translate_text()
        smt.__del__()
        json_object = {
            u"translation": out
        }
        resp = create_json_response(json_object)
        logger.debug(json_object)
    except SMTException, e:
        exit_code = e.exit_code
        str_error = "SMT Exception occurred (Language:{0}, " \
                    "Error:{1} - {2})".format(e.lang, e.exit_code, e.msg)
    except Exception, e:
        exit_code = Constants.GENERIC_EXIT_CODE
        str_error = "Exception occurred ({0})".format(str(e))
    finally:
        if exit_code:
            logger.error(str_error)
            logger.error(exit_code)
            out = {
                "translation": u"Translation Error. Please try again",
                "error": str_error,
                "exit_code": exit_code
            }
            resp = create_json_response(out)
    return resp


def create_json_response(json_object, status=200):
    """

    :param json_object: string representation of JSON object
    :return:
    """
    return jsonify(**json_object)


def _initialize_flask():
    """
    Initialize the flask app and assign it to the
    global app variable

    :rtype : Flask object
    :param app: reference to global app
    :return: flask app
    """
    logger.info('Starting flask object')
    global app
    if app:
        pass
    else:
        app = Flask(__name__)

    # http://flask.pocoo.org/docs/0.10/config/
    app.config.from_pyfile('service-stag.py')
    app.config['TESTING'] = True
    app.debug = True
    return app


if __name__ == '__main__':
    SMTLogger.setup_config("conf/logging.conf")
    app = _initialize_flask()
    app.run(host='localhost', debug=True)
