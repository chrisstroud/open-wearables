import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models import DataPointSeries, DataSource, SeriesTypeDefinition
from app.schemas.enums import ProviderName, SeriesType, get_series_type_id
from app.services.apple.healthkit.import_service import ImportService
from tests.factories import ApiKeyFactory, DataPointSeriesFactory, DataSourceFactory, UserFactory
from tests.utils import api_key_headers


def glucose_record(
    external_id: str,
    timestamp: str,
    value: str,
    source: dict[str, Any],
) -> dict[str, Any]:
    return {
        "id": external_id,
        "type": "HKQuantityTypeIdentifierBloodGlucose",
        "unit": "mg/dL",
        "value": value,
        "startDate": timestamp,
        "endDate": timestamp,
        "source": source,
    }


def test_healthkit_glucose_provenance_round_trips_without_brand_inference_or_value_logs(
    client: TestClient,
    db: Session,
    caplog: Any,
    capsys: Any,
) -> None:
    user = UserFactory()
    api_key = ApiKeyFactory()
    explicit_id = "11111111-2222-3333-4444-555555555555"
    generic_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    long_id = "99999999-8888-7777-6666-555555555555"
    private_value = "187.321"
    long_name = "N" * 180
    long_bundle = "B" * 180
    long_product = "P" * 180
    payload = {
        "provider": "apple",
        "sdkVersion": "1.0.0",
        "syncTimestamp": "2026-08-25T12:00:00Z",
        "data": {
            "records": [
                glucose_record(
                    explicit_id,
                    "2026-08-24T10:00:00Z",
                    private_value,
                    {
                        "name": "Stelo by Dexcom",
                        "bundleIdentifier": "com.dexcom.stelo",
                        "deviceManufacturer": "Dexcom",
                        "productType": "Stelo",
                    },
                ),
                glucose_record(
                    generic_id,
                    "2026-08-24T10:05:00Z",
                    "101",
                    {
                        "name": "Dexcom",
                        "bundleIdentifier": "com.dexcom.mobile",
                        "deviceManufacturer": "Dexcom",
                        "productType": "Dexcom CGM",
                    },
                ),
                glucose_record(
                    long_id,
                    "2026-08-24T10:10:00Z",
                    "99",
                    {
                        "name": long_name,
                        "bundleIdentifier": long_bundle,
                        "productType": long_product,
                    },
                ),
            ]
        },
    }
    service = ImportService(log=logging.getLogger("test.timeseries-provenance"))

    with caplog.at_level(logging.INFO, logger="test.timeseries-provenance"):
        imported = service.import_data_from_request(
            db,
            json.dumps(payload),
            "application/json",
            str(user.id),
            batch_id="12121212-3434-5656-7878-909090909090",
            require_terminal_receipt=True,
        )

    assert imported.status_code == 200
    assert imported.records_saved == 3
    assert private_value not in repr([(record.getMessage(), record.__dict__) for record in caplog.records])
    captured_output = capsys.readouterr()
    assert private_value not in captured_output.out
    assert private_value not in captured_output.err

    stored_samples = db.query(DataPointSeries).order_by(DataPointSeries.recorded_at).all()
    assert [sample.external_id for sample in stored_samples] == [explicit_id, generic_id, long_id]
    stored_sources = db.query(DataSource).order_by(DataSource.source).all()
    assert any(
        source.source == "com.dexcom.stelo"
        and source.original_source_name == "Stelo by Dexcom"
        and source.device_model == "Stelo"
        for source in stored_sources
    )
    assert any(
        source.source == "com.dexcom.mobile"
        and source.original_source_name == "Dexcom"
        and source.device_model == "Dexcom CGM"
        for source in stored_sources
    )
    long_source = next(source for source in stored_sources if source.source == long_bundle[:100])
    assert long_source.original_source_name == long_name[:100]
    assert long_source.device_model == long_product[:100]

    response = client.get(
        f"/api/v1/users/{user.id}/timeseries",
        headers=api_key_headers(api_key.id),
        params={
            "start_time": "2026-08-24T00:00:00Z",
            "end_time": "2026-08-25T00:00:00Z",
            "types": "blood_glucose",
            "limit": 10,
        },
    )

    assert response.status_code == 200
    items = response.json()["data"]
    by_id = {item["external_id"]: item for item in items}
    explicit = by_id[explicit_id]
    assert explicit["source"]["provider"] == "apple"
    assert explicit["source"]["source"] == "Stelo by Dexcom"
    assert explicit["source"]["source_identifier"] == "com.dexcom.stelo"
    assert explicit["source"]["device"] == "Stelo"
    assert explicit["source"]["device_name"] == "Stelo"

    generic = by_id[generic_id]
    assert generic["source"]["source"] == "Dexcom"
    assert generic["source"]["source_identifier"] == "com.dexcom.mobile"
    assert generic["source"]["device"] == "Dexcom CGM"
    assert "stelo" not in json.dumps(generic).lower()

    bounded = by_id[long_id]["source"]
    assert len(bounded["source"]) == 100
    assert len(bounded["source_identifier"]) == 100
    assert len(bounded["device"]) == 100


def test_bundle_provenance_adopts_legacy_source_identity_without_duplicate_replay(db: Session) -> None:
    user = UserFactory()
    sample_external_id = "abababab-cdcd-efef-1212-343434343434"
    recorded_at = datetime(2026, 8, 24, 11, 0, tzinfo=timezone.utc)
    legacy_source = DataSourceFactory(
        user=user,
        provider=ProviderName.APPLE,
        source="Stelo by Dexcom",
        original_source_name=None,
        device_model="Stelo",
    )
    series_definition = db.get(SeriesTypeDefinition, get_series_type_id(SeriesType.blood_glucose))
    assert series_definition is not None
    legacy_sample = DataPointSeriesFactory(
        data_source=legacy_source,
        series_type=series_definition,
        external_id=sample_external_id,
        recorded_at=recorded_at,
        value=Decimal("110"),
    )
    db.commit()
    legacy_source_id = legacy_source.id
    legacy_sample_id = legacy_sample.id

    service = ImportService(log=logging.getLogger("test.timeseries-provenance-adoption"))
    imported = service.import_data_from_request(
        db,
        json.dumps(
            {
                "provider": "apple",
                "sdkVersion": "1.0.0",
                "syncTimestamp": "2026-08-25T12:00:00Z",
                "data": {
                    "records": [
                        glucose_record(
                            sample_external_id,
                            recorded_at.isoformat(),
                            "110",
                            {
                                "name": "Stelo by Dexcom",
                                "bundleIdentifier": "com.dexcom.stelo",
                                "productType": "Stelo",
                            },
                        )
                    ]
                },
            }
        ),
        "application/json",
        str(user.id),
        batch_id="56565656-7878-9090-abab-cdcdcdcdcdcd",
        require_terminal_receipt=True,
    )

    assert imported.status_code == 200
    db.expire_all()
    sources = db.query(DataSource).filter(DataSource.user_id == user.id).all()
    samples = db.query(DataPointSeries).filter(DataPointSeries.external_id == sample_external_id).all()
    assert len(sources) == 1
    assert sources[0].id == legacy_source_id
    assert sources[0].source == "com.dexcom.stelo"
    assert sources[0].original_source_name == "Stelo by Dexcom"
    assert len(samples) == 1
    assert samples[0].id == legacy_sample_id
    assert samples[0].data_source_id == legacy_source_id


def test_ambiguous_legacy_and_bundle_identities_fail_closed(db: Session) -> None:
    user = UserFactory()
    DataSourceFactory(
        user=user,
        provider=ProviderName.APPLE,
        source="Dexcom",
        device_model="Dexcom CGM",
    )
    DataSourceFactory(
        user=user,
        provider=ProviderName.APPLE,
        source="com.dexcom.mobile",
        original_source_name="Dexcom",
        device_model="Dexcom CGM",
    )
    db.commit()
    service = ImportService(log=logging.getLogger("test.timeseries-provenance-conflict"))

    imported = service.import_data_from_request(
        db,
        json.dumps(
            {
                "provider": "apple",
                "sdkVersion": "1.0.0",
                "syncTimestamp": "2026-08-25T12:00:00Z",
                "data": {
                    "records": [
                        glucose_record(
                            "edededed-1212-3434-5656-787878787878",
                            "2026-08-24T12:00:00Z",
                            "105",
                            {
                                "name": "Dexcom",
                                "bundleIdentifier": "com.dexcom.mobile",
                                "productType": "Dexcom CGM",
                            },
                        )
                    ]
                },
            }
        ),
        "application/json",
        str(user.id),
        batch_id=str(uuid4()),
        require_terminal_receipt=True,
    )

    assert imported.status_code == 409
    assert imported.processing_error_code == "source_identity_conflict"
    assert (
        db.query(DataPointSeries)
        .filter(DataPointSeries.data_source_id.in_(db.query(DataSource.id).filter(DataSource.user_id == user.id)))
        .count()
        == 0
    )
    assert db.query(DataSource).filter(DataSource.user_id == user.id).count() == 2
