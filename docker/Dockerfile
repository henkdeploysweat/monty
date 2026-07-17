

FROM public.ecr.aws/lambda/python:3.12


COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt \
 && rm ${LAMBDA_TASK_ROOT}/requirements.txt


COPY lambdas/ ${LAMBDA_TASK_ROOT}/lambdas/
ENTRYPOINT ["/lambda-entrypoint.sh"]



CMD ["lambdas.observer.handler.lambda_handler"]
