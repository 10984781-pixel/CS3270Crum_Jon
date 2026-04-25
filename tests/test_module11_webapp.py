from io import BytesIO

from phase9_webapp.app import create_app


def _login(client):
    return client.post(
        "/login",
        data={"username": "student", "password": "CS3270!"},
        follow_redirects=False,
    )


def test_dashboard_requires_login():
    app = create_app()
    app.config["TESTING"] = True

    with app.test_client() as client:
        response = client.get("/dashboard")
        assert response.status_code == 302
        assert response.headers["Location"].endswith("/")


def test_login_and_upload_csv():
    app = create_app()
    app.config["TESTING"] = True

    with app.test_client() as client:
        login_response = _login(client)
        assert login_response.status_code == 302
        assert login_response.headers["Location"].endswith("/dashboard")

        csv_data = (
            b"Location,Date,MinTemp,MaxTemp,Rainfall\n"
            b"Sydney,2024-01-01,18,31,2\n"
            b"Melbourne,2024-01-02,14,26,1\n"
        )
        upload_response = client.post(
            "/api/upload",
            data={"file": (BytesIO(csv_data), "weather.csv")},
            content_type="multipart/form-data",
        )

        assert upload_response.status_code == 200
        payload = upload_response.get_json()
        assert payload["ok"] is True
        assert payload["cities"] == ["Melbourne", "Sydney"]

        analysis_response = client.get(
            "/api/analysis?category=temperature&city=Sydney"
        )
        assert analysis_response.status_code == 200
        analysis_payload = analysis_response.get_json()
        assert analysis_payload["ok"] is True
        assert "summary" in analysis_payload
