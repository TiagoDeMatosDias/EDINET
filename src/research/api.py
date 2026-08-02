"""Owner-scoped watchlist and research-note endpoints."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field

from src.auth.models import AuthenticatedUser

from .alerts import evaluate_expression
from .runtime import store

router = APIRouter(prefix="/api/research", tags=["research"])


class WatchlistRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=100)


class WatchlistItemRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    edinet_code: str = Field(min_length=1, max_length=10)
    company_name: str | None = Field(default=None, max_length=300)


class WatchlistUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=100)


class NoteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=1, max_length=200)
    body: str = Field(min_length=1, max_length=100_000)
    edinet_code: str | None = Field(default=None, max_length=10)


class NoteUpdateRequest(NoteRequest):
    expected_version: int | None = None


class CompanyResearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    thesis_status: str | None = Field(default=None, max_length=50)
    target_value: float | None = None
    target_currency: str | None = Field(default=None, max_length=10)
    review_on: str | None = Field(default=None, max_length=32)


class TagRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tags: list[str] = Field(max_length=50)


class ReorderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ordered_codes: list[str] = Field(min_length=1, max_length=200)


class SavedScreenRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=100)
    definition: dict[str, Any]


class AlertRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    edinet_code: str | None = Field(default=None, max_length=10)
    metric: str = Field(min_length=1, max_length=120)
    operator: str = Field(pattern=r"^(>|>=|<|<=|=|!=)$")
    value: float


class AlertEvaluationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    values: dict[str, Any]


class TemplateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=100)
    company_codes: list[str] = Field(min_length=2, max_length=12)
    metrics: list[str] = Field(default_factory=list, max_length=50)


class ReportRecipeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=100)
    definition: dict[str, Any]


def _user(request: Request) -> AuthenticatedUser:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser):
        raise HTTPException(status_code=401, detail="Account authentication is required")
    return user


@router.get("/watchlists")
def list_watchlists(request: Request) -> dict[str, Any]:
    return {"watchlists": store.list_watchlists(_user(request).user_id)}


@router.post("/watchlists", status_code=status.HTTP_201_CREATED)
def create_watchlist(request: Request, payload: WatchlistRequest) -> dict[str, Any]:
    try:
        return store.create_watchlist(_user(request).user_id, payload.name)
    except Exception as exc:
        if "UNIQUE" in str(exc).upper():
            raise HTTPException(status_code=409, detail="Watchlist already exists") from exc
        raise


@router.get("/watchlists/{watchlist_id}/items")
def list_watchlist_items(request: Request, watchlist_id: str) -> dict[str, Any]:
    user = _user(request)
    if store.get_watchlist(user.user_id, watchlist_id) is None:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    return {"items": store.list_watchlist_items(user.user_id, watchlist_id)}


@router.patch("/watchlists/{watchlist_id}")
def rename_watchlist(request: Request, watchlist_id: str, payload: WatchlistUpdateRequest) -> dict[str, Any]:
    user = _user(request)
    try:
        changed = store.rename_watchlist(user.user_id, watchlist_id, payload.name)
    except Exception as exc:
        if "UNIQUE" in str(exc).upper():
            raise HTTPException(status_code=409, detail="Watchlist already exists") from exc
        raise
    if not changed:
        raise HTTPException(status_code=404, detail="Watchlist not found")
    return store.get_watchlist(user.user_id, watchlist_id) or {}


@router.delete("/watchlists/{watchlist_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_watchlist(request: Request, watchlist_id: str) -> None:
    if not store.delete_watchlist(_user(request).user_id, watchlist_id):
        raise HTTPException(status_code=404, detail="Watchlist not found")


@router.post("/watchlists/{watchlist_id}/items", status_code=status.HTTP_204_NO_CONTENT)
def add_watchlist_item(request: Request, watchlist_id: str, payload: WatchlistItemRequest) -> None:
    try:
        store.add_watchlist_item(_user(request).user_id, watchlist_id, payload.edinet_code, payload.company_name)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Watchlist not found") from exc


@router.delete("/watchlists/{watchlist_id}/items/{edinet_code}", status_code=status.HTTP_204_NO_CONTENT)
def remove_watchlist_item(request: Request, watchlist_id: str, edinet_code: str) -> None:
    try:
        if not store.remove_watchlist_item(_user(request).user_id, watchlist_id, edinet_code):
            raise HTTPException(status_code=404, detail="Watchlist item not found")
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Watchlist not found") from exc


@router.get("/notes")
def list_notes(request: Request, edinet_code: str | None = None) -> dict[str, Any]:
    return {"notes": store.list_notes(_user(request).user_id, edinet_code)}


@router.post("/notes", status_code=status.HTTP_201_CREATED)
def create_note(request: Request, payload: NoteRequest) -> dict[str, Any]:
    return store.create_note(_user(request).user_id, payload.title, payload.body, payload.edinet_code)


@router.delete("/notes/{note_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_note(request: Request, note_id: str) -> None:
    if not store.delete_note(_user(request).user_id, note_id):
        raise HTTPException(status_code=404, detail="Note not found")


@router.patch("/notes/{note_id}")
def update_note(request: Request, note_id: str, payload: NoteUpdateRequest) -> dict[str, Any]:
    user = _user(request)
    ok, current_version = store.update_note(
        user.user_id, note_id, payload.title, payload.body,
        payload.edinet_code, payload.expected_version,
    )
    if not ok:
        if current_version is not None:
            raise HTTPException(
                status_code=409,
                detail=f"Note was modified by another session. Current version: {current_version}",
            )
        raise HTTPException(status_code=404, detail="Note not found")
    return next((item for item in store.list_notes(user.user_id) if item["note_id"] == note_id), {})


# -- company research --

@router.get("/companies/{edinet_code}")
def get_company_research(request: Request, edinet_code: str) -> dict[str, Any]:
    result = store.get_company_research(_user(request).user_id, edinet_code)
    if result is None:
        return {"edinet_code": edinet_code, "thesis_status": None, "target_value": None, "target_currency": None, "review_on": None, "version": 0}
    return result


@router.patch("/companies/{edinet_code}")
def update_company_research(request: Request, edinet_code: str, payload: CompanyResearchRequest) -> dict[str, Any]:
    return store.upsert_company_research(
        _user(request).user_id,
        edinet_code,
        thesis_status=payload.thesis_status,
        target_value=payload.target_value,
        target_currency=payload.target_currency,
        review_on=payload.review_on,
    )


# -- company tags --

@router.get("/tags/{edinet_code}")
def list_company_tags(request: Request, edinet_code: str) -> dict[str, Any]:
    return {"tags": store.list_company_tags(_user(request).user_id, edinet_code)}


@router.put("/tags/{edinet_code}")
def set_company_tags(request: Request, edinet_code: str, payload: TagRequest) -> dict[str, Any]:
    return {"tags": store.set_company_tags(_user(request).user_id, edinet_code, payload.tags)}


# -- watchlist member reorder --

@router.patch("/watchlists/{watchlist_id}/members/reorder")
def reorder_watchlist_members(request: Request, watchlist_id: str, payload: ReorderRequest) -> dict[str, Any]:
    try:
        store.reorder_watchlist_items(_user(request).user_id, watchlist_id, payload.ordered_codes)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Watchlist not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return store.get_watchlist(_user(request).user_id, watchlist_id) or {}


# -- saved screens --

@router.get("/screens")
def list_saved_screens(request: Request) -> dict[str, Any]:
    return {"screens": store.list_saved_screens(_user(request).user_id)}


@router.get("/recent-work")
def list_recent_work(request: Request, limit: int = 50) -> dict[str, Any]:
    user = _user(request)
    return {"items": store.list_recent_work(user.user_id, limit)}


@router.post("/screens", status_code=status.HTTP_201_CREATED)
def create_saved_screen(request: Request, payload: SavedScreenRequest) -> dict[str, Any]:
    try:
        return store.create_saved_screen(
            _user(request).user_id,
            payload.name,
            json.dumps(payload.definition, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        )
    except Exception as exc:
        if "UNIQUE" in str(exc).upper():
            raise HTTPException(status_code=409, detail="Screen already exists") from exc
        raise


@router.delete("/screens/{screen_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_saved_screen(request: Request, screen_id: str) -> None:
    if not store.delete_saved_screen(_user(request).user_id, screen_id):
        raise HTTPException(status_code=404, detail="Saved screen not found")


@router.get("/alerts")
def list_alerts(request: Request) -> dict[str, Any]:
    alerts = []
    for alert in store.list_alerts(_user(request).user_id):
        try:
            expression = json.loads(alert["expression_json"])
        except (TypeError, json.JSONDecodeError):
            expression = {}
        alerts.append({**alert, **{key: expression.get(key) for key in ("metric", "operator", "value")}})
    return {"alerts": alerts}


@router.post("/alerts", status_code=status.HTTP_201_CREATED)
def create_alert(request: Request, payload: AlertRequest) -> dict[str, Any]:
    expression = {"metric": payload.metric, "operator": payload.operator, "value": payload.value}
    return store.create_alert(
        _user(request).user_id,
        payload.name,
        payload.edinet_code,
        json.dumps(expression, separators=(",", ":")),
    )


@router.post("/alerts/{alert_id}/evaluate")
def evaluate_alert(request: Request, alert_id: str, payload: AlertEvaluationRequest) -> dict[str, Any]:
    user = _user(request)
    alert = store.alert_for_user(user.user_id, alert_id)
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    expression = json.loads(alert["expression_json"])
    triggered = evaluate_expression(expression, payload.values) if alert["enabled"] else False
    event = store.record_alert_event(
        alert_id,
        json.dumps({"values": payload.values}, separators=(",", ":")),
        dedupe_key=f"{alert_id}:{payload.values.get(expression.get('metric'))}",
    ) if triggered else None
    return {"alert_id": alert_id, "triggered": triggered, "event": event}


@router.get("/alerts/{alert_id}/events")
def list_alert_events(request: Request, alert_id: str) -> dict[str, Any]:
    user = _user(request)
    if store.alert_for_user(user.user_id, alert_id) is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    return {"events": store.list_alert_events(user.user_id, alert_id)}


@router.patch("/alert-events/{event_id}/acknowledge", status_code=status.HTTP_204_NO_CONTENT)
def acknowledge_alert_event(request: Request, event_id: str) -> None:
    if not store.acknowledge_alert_event(_user(request).user_id, event_id):
        raise HTTPException(status_code=404, detail="Alert event not found")


@router.get("/comparison-templates")
def list_comparison_templates(request: Request) -> dict[str, Any]:
    return {"templates": store.list_comparison_templates(_user(request).user_id)}


@router.post("/comparison-templates", status_code=status.HTTP_201_CREATED)
def create_comparison_template(request: Request, payload: TemplateRequest) -> dict[str, Any]:
    codes = list(dict.fromkeys(code.strip() for code in payload.company_codes if code.strip()))
    if not 2 <= len(codes) <= 12:
        raise HTTPException(status_code=400, detail="Provide between 2 and 12 company codes")
    try:
        return store.create_comparison_template(
            _user(request).user_id,
            payload.name,
            json.dumps(codes, separators=(",", ":")),
            json.dumps(payload.metrics, separators=(",", ":")),
        )
    except Exception as exc:
        if "UNIQUE" in str(exc).upper():
            raise HTTPException(status_code=409, detail="Comparison template already exists") from exc
        raise


@router.delete("/comparison-templates/{template_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_comparison_template(request: Request, template_id: str) -> None:
    if not store.delete_comparison_template(_user(request).user_id, template_id):
        raise HTTPException(status_code=404, detail="Comparison template not found")


@router.get("/report-recipes")
def list_report_recipes(request: Request) -> dict[str, Any]:
    return {"recipes": store.list_report_recipes(_user(request).user_id)}


@router.post("/report-recipes", status_code=status.HTTP_201_CREATED)
def create_report_recipe(request: Request, payload: ReportRecipeRequest) -> dict[str, Any]:
    return store.create_report_recipe(
        _user(request).user_id,
        payload.name,
        json.dumps(payload.definition, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
    )


@router.delete("/report-recipes/{recipe_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_report_recipe(request: Request, recipe_id: str) -> None:
    if not store.delete_report_recipe(_user(request).user_id, recipe_id):
        raise HTTPException(status_code=404, detail="Report recipe not found")
