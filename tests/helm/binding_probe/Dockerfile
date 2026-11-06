FROM python:3.12-slim
COPY src/libs/custos-callctx /tmp/custos-callctx
RUN pip install --no-cache-dir /tmp/custos-callctx && rm -r /tmp/custos-callctx
COPY tests/helm/binding_probe/probe.py /probe.py
RUN ln -s /probe.py /custos_workflow.py && ln -s /probe.py /custos_arm.py \
    && python -c "import custos_workflow, custos_arm"
USER 10001:10001
ENTRYPOINT ["python", "/probe.py"]
