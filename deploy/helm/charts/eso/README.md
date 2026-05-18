# eso

Renders the ClusterSecretStore used by every Custos service's ExternalSecret. The ESO operator itself must be installed out-of-band so its lifecycle is independent of Custos releases. Disabled in air-gapped profiles (Sealed Secrets is used instead).
