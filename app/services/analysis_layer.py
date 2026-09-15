"""分析层/共享层表过滤：仅用于拉取缺失中文名的 table/field。

按表英文名（entity_name_attr，默认 name）锚定正则匹配；不过滤 schema。
默认只匹配分析层前缀（ANALYSIS_LAYER_PREFIXES）；共享层前缀见 .env 注释，按需追加。
正则通过 bind 变量传入，避免 AQL 字面量里反斜杠转义导致语法/匹配错误。
"""

from app.config import get_settings


def analysis_layer_filter(var: str) -> str:
    """
    生成表名前缀/正则 FILTER 片段。

    每个配置项视为「从开头匹配」的正则，经 @al_pattern_N bind 传入：
    - dws_        -> ^dws_
    - un[0-9]+_   -> ^un[0-9]+_（精准匹配 un1_/un23_，不会命中 unit_/union_）
    """
    prefixes = get_settings().analysis_layer_prefixes_list()
    if not prefixes:
        return ""

    name_expr = f"{var}[@name_attr]"
    conditions = " OR ".join(
        f"REGEX_TEST({name_expr}, @al_pattern_{i}, true)"
        for i in range(len(prefixes))
    )
    return f"""
FILTER {var}[@name_attr] != null AND {var}[@name_attr] != "" AND (
    {conditions}
)
"""


def analysis_layer_bind_vars() -> dict:
    """与 analysis_layer_filter 配套的 bind；关闭过滤或无前缀时不注入 @al_pattern_*。"""
    settings = get_settings()
    bind: dict = {"name_attr": settings.entity_name_attr}
    if not settings.analysis_layer_filter_enabled:
        return bind
    for i, prefix in enumerate(settings.analysis_layer_prefixes_list()):
        # 不加 AQL 字面量转义；由驱动按 JSON 传字符串即可
        bind[f"al_pattern_{i}"] = f"^{prefix}"
    return bind
