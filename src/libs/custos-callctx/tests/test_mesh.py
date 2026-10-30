"""Framework-independent mesh authentication preserves duplicates and fails closed."""

import pytest

from custos_callctx.mesh import MeshAuthenticationError, require_service_caller


class Headers:
    """Provide case-insensitive duplicate-preserving values without a web-framework dependency."""

    def __init__(self, pairs: list[tuple[str, str]]) -> None:
        """Retain the original header pairs."""
        self.pairs = pairs

    def getlist(self, key: str) -> list[str]:
        """Return all values for the requested header name."""
        return [value for name, value in self.pairs if name.lower() == key.lower()]


@pytest.mark.parametrize(
    ("token", "pairs", "app_id", "namespace", "status"),
    [
        (None, [], "trigger", "custos", 503),
        (" ", [], "trigger", "custos", 503),
        ("unit-hop", [], "trigger", "custos", 401),
        ("unit-hop", [("dapr-api-token", "unit-hop")] * 2, "trigger", "custos", 401),
        ("unit-hop", [("dapr-api-token", "unit-hop")], "trigger", None, 503),
        ("unit-hop", [("dapr-api-token", "unit-hop")], "https://trigger", "custos", 503),
        ("unit-hop", [("dapr-api-token", "unit-hop")], "trigger", "custos", 403),
    ],
)
def test_mesh_failures_are_explicit(
    token: str | None,
    pairs: list[tuple[str, str]],
    app_id: str,
    namespace: str | None,
    status: int,
) -> None:
    """A missing configuration, unauthenticated hop or unauthorized caller is never accepted."""
    with pytest.raises(MeshAuthenticationError) as caught:
        require_service_caller(Headers(pairs), token=token, app_id=app_id, namespace=namespace)
    assert caught.value.status_code == status
    assert "unit-hop" not in str(caught.value)


def test_identity_requires_exact_app_namespace_and_single_values() -> None:
    """Only one exact authenticated application/namespace pair passes."""
    pairs = [
        ("Dapr-Api-Token", "unit-hop"),
        ("Dapr-Caller-App-Id", "trigger"),
        ("Dapr-Caller-Namespace", "custos"),
    ]
    require_service_caller(Headers(pairs), token="unit-hop", app_id="trigger", namespace="custos")
    for changed in (
        [*pairs, ("dapr-caller-app-id", "trigger")],
        [*pairs[:-1], ("dapr-caller-namespace", "other")],
    ):
        with pytest.raises(MeshAuthenticationError, match="identity required"):
            require_service_caller(
                Headers(changed), token="unit-hop", app_id="trigger", namespace="custos"
            )
