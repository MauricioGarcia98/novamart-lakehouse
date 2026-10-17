"""Reglas de calidad por tabla silver (contratos de datos)."""

from novamart.quality.expectations import Rule

CUSTOMERS = [
    Rule("customer_id_not_null", "customer_id IS NOT NULL", "quarantine"),
    Rule("updated_at_parseable", "updated_at IS NOT NULL", "quarantine"),
    Rule("email_format", "is_deleted OR email RLIKE '^[a-z0-9._%+-]+@[a-z0-9.-]+\\\\.[a-z]{2,}$'", "warn"),
    Rule("country_valid", "country IN ('AR','CL','UY','MX','CO','PE')", "warn"),
]

PRODUCTS = [
    Rule("product_id_not_null", "product_id IS NOT NULL", "fail"),
    Rule("price_positive", "list_price > 0", "quarantine"),
    Rule("cost_below_price", "unit_cost <= list_price", "warn"),
]

ORDERS = [
    Rule("order_ts_parseable", "order_ts IS NOT NULL", "quarantine"),
    Rule("channel_valid", "channel IN ('WEB','APP','STORE','MARKETPLACE')", "quarantine"),
    Rule("online_order_has_customer", "channel = 'STORE' OR customer_id IS NOT NULL", "quarantine"),
    Rule("store_order_has_store", "channel <> 'STORE' OR store_id IS NOT NULL", "quarantine"),
    Rule("has_items", "n_items > 0", "quarantine"),
    Rule("not_in_future", "order_date <= current_date()", "warn"),
]

ORDER_LINES = [
    Rule("quantity_positive", "quantity > 0", "quarantine"),
    Rule("price_not_negative", "unit_price >= 0", "quarantine"),
    Rule("discount_range", "discount_pct BETWEEN 0 AND 0.9", "warn"),
]

STATUS_EVENTS = [
    Rule("status_valid",
         "status IN ('CREATED','PAYMENT_APPROVED','CANCELLED','SHIPPED','READY_FOR_PICKUP','DELIVERED')",
         "quarantine"),
    Rule("status_ts_parseable", "status_ts IS NOT NULL", "quarantine"),
]

RETURNS = [
    Rule("refund_positive", "refund_amount > 0", "quarantine"),
    Rule("quantity_positive", "quantity > 0", "quarantine"),
]

WEB_EVENTS = [
    Rule("event_type_valid",
         "event_type IN ('page_view','search','product_view','add_to_cart','begin_checkout','purchase')",
         "quarantine"),
    Rule("session_not_null", "session_id IS NOT NULL", "quarantine"),
]

INVENTORY = [
    Rule("stock_not_negative", "on_hand_qty >= 0", "quarantine"),
]
