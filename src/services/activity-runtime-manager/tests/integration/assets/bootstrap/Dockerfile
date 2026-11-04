FROM custos-sidecar-e2e:test
COPY probe.py /opt/bootstrap-probe.py
USER 65532:65532
ENTRYPOINT ["python", "/opt/bootstrap-probe.py"]
