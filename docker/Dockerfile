

FROM public.ecr.aws/lambda/python:3.12


COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt \
 && rm ${LAMBDA_TASK_ROOT}/requirements.txt


COPY lambdas/ ${LAMBDA_TASK_ROOT}/lambdas/

# The rollup Lambda reuses the dashboard's aggregation + DynamoDB reader, so
# there is ONE implementation of the rollup schema instead of a copy that can
# drift from what the dashboard reads. Only these two modules are needed and
# both import stdlib + boto3 only; the rest of dash/ is the Flask app and
# stays out of the image. rollup.py does `import db` with its own directory on
# sys.path, so the two must land in the SAME directory.
COPY dash/db.py dash/rollup.py ${LAMBDA_TASK_ROOT}/dash/

ENTRYPOINT ["/lambda-entrypoint.sh"]



CMD ["lambdas.observer.handler.lambda_handler"]
