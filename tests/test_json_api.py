"""Tests for the JSON client API methods."""

from typing import Any

import httpx
import pytest
import respx

from pymycorr import (
    AuthenticationError,
    ForbiddenError,
    JsonAPIError,
    MyCorr,
    NotFoundError,
)

BASE_URL = "https://test.example.com/api/data/json/v1"

EMPTY_DATA_RESPONSE: dict[str, Any] = {
    "data": {"table_id": "tbl-1", "name": "T", "created_at": 0, "columns": []},
    "meta": {},
}


class TestJsonApiRequest:
    """Tests for the _json_api_request private helper."""

    @respx.mock
    def test_auth_header_sent(self, client: MyCorr) -> None:
        route = respx.get(f"{BASE_URL}/models").mock(
            return_value=httpx.Response(200, json={"data": []})
        )

        client.list_models()

        assert route.called
        request = route.calls.last.request
        assert request.headers["Authorization"] == "Bearer test-token"

    @respx.mock
    def test_error_without_json_body(self, client: MyCorr) -> None:
        respx.get(f"{BASE_URL}/models").mock(
            return_value=httpx.Response(500, text="Internal Server Error")
        )

        with pytest.raises(JsonAPIError, match="HTTP 500"):
            client.list_models()

    @respx.mock
    def test_401_raises_authentication_error(self, client: MyCorr) -> None:
        respx.get(f"{BASE_URL}/models").mock(
            return_value=httpx.Response(
                401,
                json={"error": {"status": 401, "message": "Invalid token"}},
            )
        )

        with pytest.raises(AuthenticationError, match="Invalid token"):
            client.list_models()

    @respx.mock
    def test_403_raises_forbidden_error(self, client: MyCorr) -> None:
        respx.get(url__startswith=f"{BASE_URL}/models/mdl-1").mock(
            return_value=httpx.Response(
                403,
                json={"error": {"status": 403, "message": "Insufficient permissions"}},
            )
        )

        with pytest.raises(ForbiddenError, match="Insufficient permissions"):
            client.get_model("mdl-1")

    @respx.mock
    def test_404_raises_table_not_found(self, client: MyCorr) -> None:
        respx.get(url__startswith=f"{BASE_URL}/models/mdl-missing").mock(
            return_value=httpx.Response(
                404,
                json={"error": {"status": 404, "message": "Model not found"}},
            )
        )

        with pytest.raises(NotFoundError, match="Model not found"):
            client.get_model("mdl-missing")


class TestListModels:
    """Tests for list_models."""

    @respx.mock
    def test_success(
        self, client: MyCorr, sample_model_metadata: dict[str, Any]
    ) -> None:
        respx.get(f"{BASE_URL}/models").mock(
            return_value=httpx.Response(200, json={"data": [sample_model_metadata]})
        )

        result = client.list_models()

        assert len(result) == 1
        assert result[0]["model_id"] == "mdl-test-1234"
        assert result[0]["model_name"] == "Test Model"

    @respx.mock
    def test_empty_list(self, client: MyCorr) -> None:
        respx.get(f"{BASE_URL}/models").mock(
            return_value=httpx.Response(200, json={"data": []})
        )

        result = client.list_models()
        assert result == []


class TestGetModel:
    """Tests for get_model."""

    @respx.mock
    def test_success(
        self, client: MyCorr, sample_model_metadata: dict[str, Any]
    ) -> None:
        respx.get(url__startswith=f"{BASE_URL}/models/mdl-test-1234").mock(
            return_value=httpx.Response(200, json={"data": sample_model_metadata})
        )

        result = client.get_model("mdl-test-1234")

        assert result["model_id"] == "mdl-test-1234"
        assert result["description"] == "A test model"

    def test_empty_id_raises(self, client: MyCorr) -> None:
        with pytest.raises(ValueError, match="model_id is required"):
            client.get_model("")


class TestListTables:
    """Tests for list_tables."""

    @respx.mock
    def test_success(
        self, client: MyCorr, sample_table_info: dict[str, Any]
    ) -> None:
        respx.get(url__startswith=f"{BASE_URL}/models/mdl-1/tables").mock(
            return_value=httpx.Response(200, json={"data": [sample_table_info]})
        )

        result = client.list_tables("mdl-1")

        assert len(result) == 1
        assert result[0]["table_id"] == "tbl-test-5678"
        assert result[0]["name"] == "Customers"
        assert result[0]["latest_version"] == 12

    def test_empty_model_id_raises(self, client: MyCorr) -> None:
        with pytest.raises(ValueError, match="model_id is required"):
            client.list_tables("")


class TestGetTableSchema:
    """Tests for get_table_schema."""

    @respx.mock
    def test_success(self, client: MyCorr) -> None:
        table_metadata = {
            "table_id": "tbl-1",
            "name": "Users",
            "shape": "Table",
            "version": 5,
            "row_count": 100,
            "column_count": 3,
            "created_at": 1706572800,
            "last_modified": 1709251200,
            "columns": [
                {"id": "col-1", "name": "Email", "data_type": "Utf8", "is_nullable": False},
                {"id": "col-2", "name": "Revenue", "data_type": "Float64", "is_nullable": True},
                {"id": "col-3", "name": "Active", "data_type": "Boolean", "is_nullable": False},
            ],
        }
        respx.get(url__startswith=f"{BASE_URL}/tables/tbl-1").mock(
            return_value=httpx.Response(200, json={"data": table_metadata})
        )

        result = client.get_table_schema("tbl-1")

        assert result["table_id"] == "tbl-1"
        assert result["name"] == "Users"
        assert result["version"] == 5
        assert result["row_count"] == 100
        assert result["column_count"] == 3
        assert len(result["columns"]) == 3
        assert result["columns"][0]["name"] == "Email"
        assert result["columns"][1]["is_nullable"] is True

    def test_empty_table_id_raises(self, client: MyCorr) -> None:
        with pytest.raises(ValueError, match="table_id is required"):
            client.get_table_schema("")


class TestListCheckpoints:
    """Tests for list_checkpoints."""

    @respx.mock
    def test_success(self, client: MyCorr) -> None:
        checkpoints = [
            {
                "name": "v3-snapshot",
                "aliases": ["stable"],
                "version": 3,
                "created_at": 1709251200,
                "version_timestamp": 1709251200,
            },
            {
                "name": "v1-initial",
                "aliases": [],
                "version": 1,
                "created_at": 1706572800,
                "version_timestamp": 1706572800,
            },
        ]
        respx.get(url__startswith=f"{BASE_URL}/tables/tbl-1/checkpoints").mock(
            return_value=httpx.Response(200, json={"data": checkpoints})
        )

        result = client.list_checkpoints("tbl-1")

        assert len(result) == 2
        assert result[0]["name"] == "v3-snapshot"
        assert result[0]["aliases"] == ["stable"]
        assert result[0]["version"] == 3
        assert result[1]["name"] == "v1-initial"

    @respx.mock
    def test_empty_list(self, client: MyCorr) -> None:
        respx.get(url__startswith=f"{BASE_URL}/tables/tbl-1/checkpoints").mock(
            return_value=httpx.Response(200, json={"data": []})
        )

        result = client.list_checkpoints("tbl-1")
        assert result == []

    def test_empty_table_id_raises(self, client: MyCorr) -> None:
        with pytest.raises(ValueError, match="table_id is required"):
            client.list_checkpoints("")


class TestGetTableData:
    """Tests for get_table_data."""

    @respx.mock
    def test_success(
        self, client: MyCorr, sample_table_data_page: dict[str, Any]
    ) -> None:
        respx.get(url__startswith=f"{BASE_URL}/tables/tbl-1/data").mock(
            return_value=httpx.Response(200, json=sample_table_data_page)
        )

        result = client.get_table_data("tbl-1")

        assert result["data"]["table_id"] == "tbl-test-5678"
        assert len(result["data"]["columns"]) == 2
        assert result["meta"]["total_rows"] == 2

    @respx.mock
    def test_with_version_param(self, client: MyCorr) -> None:
        route = respx.get(url__startswith=f"{BASE_URL}/tables/tbl-1/data").mock(
            return_value=httpx.Response(200, json=EMPTY_DATA_RESPONSE)
        )

        client.get_table_data("tbl-1", version=3)

        request = route.calls.last.request
        assert "version=3" in str(request.url)

    @respx.mock
    def test_with_checkpoint_param(self, client: MyCorr) -> None:
        route = respx.get(url__startswith=f"{BASE_URL}/tables/tbl-1/data").mock(
            return_value=httpx.Response(200, json=EMPTY_DATA_RESPONSE)
        )

        client.get_table_data("tbl-1", table_checkpoint="stable")

        request = route.calls.last.request
        assert "table_checkpoint=stable" in str(request.url)

    def test_version_and_checkpoint_raises(self, client: MyCorr) -> None:
        with pytest.raises(ValueError, match="Cannot specify both"):
            client.get_table_data("tbl-1", version=1, table_checkpoint="stable")

    def test_empty_table_id_raises(self, client: MyCorr) -> None:
        with pytest.raises(ValueError, match="table_id is required"):
            client.get_table_data("")

    @respx.mock
    def test_with_limit_and_page_token(self, client: MyCorr) -> None:
        route = respx.get(url__startswith=f"{BASE_URL}/tables/tbl-1/data").mock(
            return_value=httpx.Response(200, json=EMPTY_DATA_RESPONSE)
        )

        client.get_table_data("tbl-1", limit=25, page_token="abc123")

        request = route.calls.last.request
        url_str = str(request.url)
        assert "limit=25" in url_str
        assert "page_token=abc123" in url_str


class TestIterTablePages:
    """Tests for iter_table_pages."""

    @respx.mock
    def test_single_page(
        self, client: MyCorr, sample_table_data_page: dict[str, Any]
    ) -> None:
        respx.get(url__startswith=f"{BASE_URL}/tables/tbl-1/data").mock(
            return_value=httpx.Response(200, json=sample_table_data_page)
        )

        pages = list(client.iter_table_pages("tbl-1"))

        assert len(pages) == 1
        assert pages[0]["data"]["table_id"] == "tbl-test-5678"

    @respx.mock
    def test_multi_page(self, client: MyCorr) -> None:
        page1 = {
            "data": {
                "table_id": "tbl-1",
                "name": "T",
                "created_at": 0,
                "columns": [
                    {"id": "c1", "name": "val", "data_type": "Int32", "data": [1, 2]},
                ],
            },
            "meta": {"next_page_token": "token-page-2", "total_rows": 4, "total_columns": 1},
        }
        page2 = {
            "data": {
                "table_id": "tbl-1",
                "name": "T",
                "created_at": 0,
                "columns": [
                    {"id": "c1", "name": "val", "data_type": "Int32", "data": [3, 4]},
                ],
            },
            "meta": {"total_rows": 4, "total_columns": 1},
        }

        route = respx.get(url__startswith=f"{BASE_URL}/tables/tbl-1/data")
        route.side_effect = [
            httpx.Response(200, json=page1),
            httpx.Response(200, json=page2),
        ]

        pages = list(client.iter_table_pages("tbl-1"))

        assert len(pages) == 2
        assert pages[0]["data"]["columns"][0]["data"] == [1, 2]
        assert pages[1]["data"]["columns"][0]["data"] == [3, 4]

    def test_version_and_checkpoint_raises(self, client: MyCorr) -> None:
        with pytest.raises(ValueError, match="Cannot specify both"):
            list(client.iter_table_pages("tbl-1", version=1, table_checkpoint="s"))


class TestGetTableDataframe:
    """Tests for get_table_dataframe."""

    @respx.mock
    def test_pandas_dataframe(
        self, client: MyCorr, sample_table_data_page: dict[str, Any]
    ) -> None:
        respx.get(url__startswith=f"{BASE_URL}/tables/tbl-1/data").mock(
            return_value=httpx.Response(200, json=sample_table_data_page)
        )

        df = client.get_table_dataframe("tbl-1", engine="pandas")

        assert list(df.columns) == ["Email", "Revenue"]
        assert len(df) == 2
        assert df["Email"].tolist() == ["alice@test.com", "bob@test.com"]

    @respx.mock
    def test_polars_dataframe(
        self, client: MyCorr, sample_table_data_page: dict[str, Any]
    ) -> None:
        pytest.importorskip("polars")
        respx.get(url__startswith=f"{BASE_URL}/tables/tbl-1/data").mock(
            return_value=httpx.Response(200, json=sample_table_data_page)
        )

        df = client.get_table_dataframe("tbl-1", engine="polars")

        assert df.columns == ["Email", "Revenue"]
        assert len(df) == 2

    @respx.mock
    def test_multi_page_concatenation(self, client: MyCorr) -> None:
        page1 = {
            "data": {
                "table_id": "tbl-1",
                "name": "T",
                "created_at": 0,
                "columns": [
                    {"id": "c1", "name": "val", "data_type": "Int32", "data": [1, 2]},
                ],
            },
            "meta": {"next_page_token": "tok2", "total_rows": 4, "total_columns": 1},
        }
        page2 = {
            "data": {
                "table_id": "tbl-1",
                "name": "T",
                "created_at": 0,
                "columns": [
                    {"id": "c1", "name": "val", "data_type": "Int32", "data": [3, 4]},
                ],
            },
            "meta": {"total_rows": 4, "total_columns": 1},
        }

        route = respx.get(url__startswith=f"{BASE_URL}/tables/tbl-1/data")
        route.side_effect = [
            httpx.Response(200, json=page1),
            httpx.Response(200, json=page2),
        ]

        df = client.get_table_dataframe("tbl-1", engine="pandas")

        assert len(df) == 4
        assert df["val"].tolist() == [1, 2, 3, 4]

    def test_invalid_engine_raises(self, client: MyCorr) -> None:
        with pytest.raises(ValueError, match="Engine must be"):
            client.get_table_dataframe("tbl-1", engine="spark")  # type: ignore[arg-type]

    @respx.mock
    def test_empty_table(self, client: MyCorr) -> None:
        respx.get(url__startswith=f"{BASE_URL}/tables/tbl-1/data").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "table_id": "tbl-1",
                        "name": "T",
                        "created_at": 0,
                        "columns": [],
                    },
                    "meta": {"total_rows": 0, "total_columns": 0},
                },
            )
        )

        df = client.get_table_dataframe("tbl-1", engine="pandas")

        assert len(df) == 0
        assert len(df.columns) == 0
