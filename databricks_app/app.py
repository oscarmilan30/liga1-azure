"""
app.py — Streamlit UI para el asistente RAG de Liga 1 Perú
"""

import os
import base64
import streamlit as st
from rag import responder

st.set_page_config(
    page_title="Asistente Liga 1 Perú",
    page_icon="⚽",
    layout="centered",
)

st.markdown("""
<style>
  .block-container { padding-top: 6rem; padding-bottom: 0.5rem; max-width: 780px; }
  div[data-testid="stVerticalBlock"] { gap: 0.4rem; }
  .stButton > button { font-size: 0.82rem; padding: 0.3rem 0.6rem; }
</style>
""", unsafe_allow_html=True)

# ─── Header con logo ──────────────────────────────────────────────────────────

logo_path = os.path.join(os.path.dirname(__file__), "img", "logo.png")
col_logo, col_titulo = st.columns([1, 3])
with col_logo:
    if os.path.exists(logo_path):
        st.image(logo_path, width=140)
    else:
        st.markdown("⚽")
with col_titulo:
    st.markdown("### Asistente Liga 1 Perú")
    st.caption("Consulta estadísticas, posiciones y rendimiento con lenguaje natural.")

st.divider()

# ─── Ejemplos ─────────────────────────────────────────────────────────────────

st.markdown("**💡 Ejemplos:**")
ejemplos = [
    "¿Cuál es la tabla de posiciones actual?",
    "¿Cuál fue la mayor goleada de 2025?",
    "¿Cuál es el XI ideal de Liga 1?",
    "¿Quiénes son los máximos goleadores de 2026?",
    "Sporting Cristal vs Alianza Lima",
    "¿Quién fue el mejor entrenador de Alianza Lima?",
    "Evolución de puntos de Universitario",
    "Valorización de Sporting Cristal del 2020 al 2026",
]
col1, col2 = st.columns(2)
for i, ej in enumerate(ejemplos):
    with col1 if i % 2 == 0 else col2:
        if st.button(ej, key=f"ej_{i}", use_container_width=True):
            st.session_state["pregunta_rapida"] = ej

st.divider()

# ─── Helpers de detección ─────────────────────────────────────────────────────

def _pide_tabla(pregunta: str) -> bool:
    p = pregunta.lower()
    return any(w in p for w in [
        "muéstrame la tabla", "dame la tabla", "tabla detallada",
        "en tabla", "listado", "lista completa", "detalle completo",
        "todos los datos", "dame los datos",
    ])

def _pide_grafico(pregunta: str) -> tuple[bool, str]:
    p = pregunta.lower()
    keywords_graf = ["gráfico", "grafico", "gráfica", "grafica", "chart",
                     "visualiza", "barra", "barras", "línea", "linea", "líneas", "lineas", "radar"]
    if not any(w in p for w in keywords_graf):
        return False, "bar"
    # Prioridad: el tipo explícito mencionado gana
    if any(w in p for w in ["barra", "barras"]):
        tipo = "bar"
    elif any(w in p for w in ["línea", "linea", "líneas", "lineas", "evolucion", "evolución",
                                "tendencia", "histórico", "historico"]):
        tipo = "line"
    elif any(w in p for w in ["radar"]):
        tipo = "radar"
    elif any(w in p for w in ["comparar", "compara", "vs", "versus"]):
        tipo = "radar"
    else:
        tipo = "bar"
    return True, tipo

def _es_xi_ideal(pregunta: str) -> bool:
    import re as _re
    p = pregunta.lower()
    return (any(w in p for w in ["xi ideal", "mejor xi", "once ideal", "11 ideal",
                                   "formacion ideal", "formación ideal"])
            or bool(_re.search(r'\bxi\b', p)))

def _es_vs(pregunta: str) -> bool:
    p = pregunta.lower()
    return any(w in p for w in [" vs ", " versus ", "compara", "comparar", "contra"])


# ─── Carga de fotos con cache ─────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def _load_foto_cached(url: str) -> str | None:
    """Descarga y codifica en base64 la foto de un jugador. Cachea 1 hora."""
    if not url or not url.startswith("http"):
        return None
    try:
        import urllib.request
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=4) as r:
            return base64.b64encode(r.read()).decode("utf-8")
    except Exception:
        return None


# ─── Gráficos ─────────────────────────────────────────────────────────────────

def _render_grafico(df, tipo: str):
    """Renderiza el gráfico adecuado según tipo y columnas disponibles.
    Usa plotly express para líneas (eje X explícito, sin coma en años).
    """
    import plotly.express as px

    cols = set(df.columns)

    def _line_fig(x, y, color=None, title="", ylabel=""):
        """Helper: línea plotly con eje X sin coma y sin decimales."""
        df_sorted = df.sort_values(x)
        fig = px.line(
            df_sorted, x=x, y=y, color=color,
            title=title,
            labels={x: x.replace("_", " ").title(), y: ylabel or y},
            markers=True,
        )
        fig.update_xaxes(tickformat="d", dtick=1)   # años como "2020" no "2,020"
        fig.update_layout(
            height=380,
            margin=dict(l=40, r=20, t=50, b=40),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0.03)",
            legend=dict(orientation="h", y=-0.25),
        )
        return fig

    if tipo == "line":
        if "temporada" in cols and "puntos" in cols and "nombre_equipo" in cols:
            fig = _line_fig("temporada", "puntos", color="nombre_equipo",
                            title="📈 Evolución de puntos por temporada", ylabel="Puntos")
            st.plotly_chart(fig, use_container_width=True)

        elif "temporada" in cols and "score_100" in cols and "jugador" in cols:
            fig = _line_fig("temporada", "score_100", color="jugador",
                            title="📈 Evolución Score ML", ylabel="Score ML")
            st.plotly_chart(fig, use_container_width=True)

        elif "temporada" in cols and "valor_total_mm_eur" in cols:
            fig = _line_fig("temporada", "valor_total_mm_eur",
                            title="📈 Valor total de la liga (M€)", ylabel="Valor (M€)")
            st.plotly_chart(fig, use_container_width=True)

        elif "temporada" in cols and "valor_total" in cols and "nombre_equipo" in cols:
            df_v = df.copy()
            df_v["valor_mm"] = (df_v["valor_total"] / 1_000_000).round(2)
            fig = _line_fig("temporada", "valor_mm", color="nombre_equipo",
                            title="📈 Evolución del valor de mercado", ylabel="Valor (MM €)")
            st.plotly_chart(fig, use_container_width=True)

    elif tipo == "bar":
        if "jugador" in cols and "goles" in cols:
            st.subheader("⚽ Goles por jugador")
            st.bar_chart(df.set_index("jugador")["goles"])

        elif "jugador" in cols and "score_100" in cols and "temporada" not in cols:
            st.subheader("🏆 Score ML por jugador")
            st.bar_chart(df.set_index("jugador")["score_100"])

        elif "nombre_equipo" in cols and "ppp_historico" in cols:
            st.subheader("📊 Eficiencia histórica (PPP)")
            st.bar_chart(df.set_index("nombre_equipo")["ppp_historico"])

        elif "nombre_equipo" in cols and "precision_tiro_pct" in cols:
            st.subheader("🎯 Precisión de tiro por equipo")
            st.bar_chart(df.set_index("nombre_equipo")["precision_tiro_pct"])

        elif "entrenador" in cols and "ppp" in cols:
            st.subheader("🏅 Entrenadores por PPP")
            st.bar_chart(df.set_index("entrenador")["ppp"])

        elif "nombre_equipo" in cols and "puntos" in cols:
            st.subheader("📊 Puntos por equipo")
            st.bar_chart(df.set_index("nombre_equipo")["puntos"])

        elif "nombre_equipo" in cols and "valor_total" in cols:
            # Escalar a millones de EUR para valores legibles
            import plotly.graph_objects as go_bar
            df_v = df.copy()
            df_v["valor_mm"] = (df_v["valor_total"] / 1_000_000).round(2)
            if "temporada" in cols:
                # Serie temporal: usar go.Bar con eje X en string (evita freeze de px.bar con int)
                df_s = df_v.sort_values("temporada")
                df_s["año"] = df_s["temporada"].astype(str)
                COLORES = ["#00BFFF", "#FFD700", "#90EE90", "#FFA07A", "#FF6B6B", "#DA70D6"]
                fig_bar = go_bar.Figure()
                equipos_u = df_s["nombre_equipo"].unique()
                for i, eq in enumerate(equipos_u):
                    d = df_s[df_s["nombre_equipo"] == eq]
                    fig_bar.add_trace(go_bar.Bar(
                        x=d["año"].tolist(),
                        y=d["valor_mm"].tolist(),
                        name=eq.title(),
                        marker_color=COLORES[i % len(COLORES)],
                        text=[f"{v:.1f}" for v in d["valor_mm"].tolist()],
                        textposition="outside",
                    ))
                fig_bar.update_layout(
                    title="💰 Valor de mercado (MM €)",
                    xaxis_title="Temporada",
                    yaxis_title="Valor (MM €)",
                    barmode="group",
                    height=380,
                    margin=dict(l=40, r=20, t=50, b=40),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0.03)",
                    legend=dict(orientation="h", y=-0.25),
                )
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.subheader("💰 Valor de mercado por equipo (MM €)")
                st.bar_chart(df_v.set_index("nombre_equipo")["valor_mm"])


def _render_butterfly_vs(df, group_col: str = "jugador"):
    """Butterfly chart estilo Power BI: barras que se abren en espejo desde el centro.
    Entidad 1 → barras rojas hacia la izquierda.
    Entidad 2 → barras azules hacia la derecha.
    Funciona para jugadores, equipos y entrenadores.
    """
    import plotly.graph_objects as go

    if df is None or df.empty or group_col not in df.columns:
        return

    entidades = df[group_col].unique()[:2]
    if len(entidades) < 2:
        return

    # Métricas según el tipo de entidad
    METRICAS = {
        "jugador": [
            ("score_100",        "Score ML"),
            ("goles",            "Goles"),
            ("asistencias",      "Asistencias"),
            ("minutos_jugados",  "Minutos"),
            ("partidos_jugados", "Partidos"),
            ("ppp",              "PPP"),
        ],
        "nombre_equipo": [
            ("victorias",               "Victorias"),
            ("goles_marcados",          "Goles marc."),
            ("goles_recibidos",         "Goles recib."),
            ("posesion_media_pct",      "Posesión %"),
            ("tiros_promedio",          "Tiros prom."),
            ("empates",                 "Empates"),
            ("amarillas_promedio",      "Amarillas"),
        ],
        "entrenador": [
            ("ppp_promedio",        "PPP prom."),
            ("mejor_ppp_temporada", "Mejor PPP"),
            ("partidos_totales",    "Partidos"),
            ("temporadas",          "Temporadas"),
            ("equipos_dirigidos",   "Equipos"),
        ],
    }
    metricas_all = METRICAS.get(group_col, METRICAS["jugador"])
    metricas_disp = [(k, lbl) for k, lbl in metricas_all if k in df.columns]
    if not metricas_disp:
        return

    def get_row(ent):
        rows = df[df[group_col] == ent]
        if "temporada" in rows.columns and len(rows) > 1:
            rows = rows.sort_values("temporada", ascending=False)
        return rows.iloc[0]

    row1 = get_row(entidades[0])
    row2 = get_row(entidades[1])
    labels = [lbl for _, lbl in metricas_disp]
    vals1  = [float(row1.get(k, 0)) for k, _ in metricas_disp]
    vals2  = [float(row2.get(k, 0)) for k, _ in metricas_disp]

    # Normalizar cada métrica al máximo de los dos → barra proporcional 0-100
    maxes  = [max(abs(v1), abs(v2), 0.01) for v1, v2 in zip(vals1, vals2)]
    norm1  = [v / m * 100 for v, m in zip(vals1, maxes)]
    norm2  = [v / m * 100 for v, m in zip(vals2, maxes)]

    def fmt(v):
        if v != v: return "0"          # NaN guard
        if v == int(v): return str(int(v))
        return f"{v:.2f}" if abs(v) < 10 else f"{v:.1f}"

    temp1 = str(int(row1["temporada"])) if "temporada" in df.columns and row1.get("temporada") else ""
    temp2 = str(int(row2["temporada"])) if "temporada" in df.columns and row2.get("temporada") else ""
    subtitle = ""
    if temp1 and temp2:
        subtitle = f" · {temp1}" if temp1 == temp2 else f" · {temp1} | {temp2}"

    fig = go.Figure()
    # Entidad 1 → izquierda (x negativo)
    fig.add_trace(go.Bar(
        name=str(entidades[0]).title(),
        y=labels,
        x=[-n for n in norm1],
        orientation="h",
        marker_color="#e84545",
        text=[fmt(v) for v in vals1],
        textposition="outside",
        constraintext="none",
        hovertemplate="%{y}: %{text}<extra>" + str(entidades[0]).title() + "</extra>",
    ))
    # Entidad 2 → derecha (x positivo)
    fig.add_trace(go.Bar(
        name=str(entidades[1]).title(),
        y=labels,
        x=norm2,
        orientation="h",
        marker_color="#00BFFF",
        text=[fmt(v) for v in vals2],
        textposition="outside",
        constraintext="none",
        hovertemplate="%{y}: %{text}<extra>" + str(entidades[1]).title() + "</extra>",
    ))

    fig.update_layout(
        barmode="overlay",
        title=dict(
            text=f"⚔️ {str(entidades[0]).title()} vs {str(entidades[1]).title()}{subtitle}",
            x=0.5, xanchor="center",
        ),
        xaxis=dict(
            range=[-138, 138],
            showticklabels=False,
            zeroline=True,
            zerolinecolor="rgba(180,180,180,0.9)",
            zerolinewidth=2,
            showgrid=False,
            fixedrange=True,
        ),
        yaxis=dict(showgrid=False, fixedrange=True, tickfont=dict(size=11)),
        height=max(280, 55 + len(labels) * 48),
        legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center", font=dict(size=11)),
        margin=dict(l=15, r=15, t=68, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(size=11),
    )
    st.plotly_chart(fig, use_container_width=True)


# Alias de compatibilidad (para no romper llamadas existentes en _render_data)
def _render_bar_vs(df):
    _render_butterfly_vs(df, group_col="nombre_equipo")


def _render_radar(df):
    """Radar chart para comparar dos jugadores por sus métricas."""
    import plotly.graph_objects as go

    if df is None or df.empty or "jugador" not in df.columns:
        return

    # Métricas a comparar (solo las que existen)
    candidatas = ["goles", "asistencias", "partidos_jugados", "minutos_jugados",
                  "score_100", "ppp", "tarjetas_amarillas"]
    metricas = [m for m in candidatas if m in df.columns]
    if len(metricas) < 3:
        return

    etiquetas = {
        "goles": "Goles", "asistencias": "Asistencias",
        "partidos_jugados": "Partidos", "minutos_jugados": "Minutos",
        "score_100": "Score ML", "ppp": "PPP", "tarjetas_amarillas": "Amarillas",
    }

    # Agrupar por jugador (puede haber múltiples temporadas → tomar max reciente)
    jugadores = df["jugador"].unique()
    colores_line = ["#FFD700", "#00BFFF"]
    colores_fill = ["rgba(255,215,0,0.2)", "rgba(0,191,255,0.2)"]

    fig = go.Figure()

    # Normalizar valores al máximo del df para escala 0-100
    maximos = {m: max(df[m].max(), 1) for m in metricas}

    for jugador, color_l, color_f in zip(jugadores[:2], colores_line, colores_fill):
        fila = df[df["jugador"] == jugador].sort_values("temporada", ascending=False).iloc[0]
        vals = [float(fila.get(m, 0)) / maximos[m] * 100 for m in metricas]
        vals += vals[:1]  # cerrar el polígono
        cats = [etiquetas.get(m, m) for m in metricas] + [etiquetas.get(metricas[0], metricas[0])]

        fig.add_trace(go.Scatterpolar(
            r=vals,
            theta=cats,
            fill="toself",
            fillcolor=color_f,
            line=dict(color=color_l, width=2),
            name=jugador.title(),
        ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100],
                            ticksuffix="%", showticklabels=True),
        ),
        title="🔍 Comparativa de jugadores",
        showlegend=True,
        legend=dict(orientation="h", y=-0.15),
        height=420,
        margin=dict(l=40, r=40, t=60, b=60),
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)


# ─── XI Ideal en campo ────────────────────────────────────────────────────────

def _render_xi_campo(df):
    """Renderiza el XI ideal sobre un campo de fútbol usando plotly.
    Usa x_coord e y_coord de ft_score_ml_vw (mismas que Power BI).
    Fotos cacheadas de foto_url si están disponibles.
    """
    import plotly.graph_objects as go

    fig = go.Figure()

    VERDE   = "#2d7a27"
    VERDE_L = "#348a2d"
    BLANCO  = "white"

    # ── Fondo y líneas: TODAS con layer="below" para no tapar los scatter traces ──
    # Césped base
    fig.add_shape(type="rect", x0=0, y0=0, x1=100, y1=100,
                  fillcolor=VERDE, line_color=BLANCO, line_width=2, layer="below")
    # Franjas alternadas
    for i in range(0, 100, 10):
        if i % 20 == 0:
            fig.add_shape(type="rect", x0=0, y0=i, x1=100, y1=i+10,
                          fillcolor=VERDE_L, line_width=0, layer="below")
    # Líneas del campo
    fig.add_shape(type="line",   x0=0,  y0=50, x1=100, y1=50, line=dict(color=BLANCO, width=1), layer="below")
    fig.add_shape(type="circle", x0=40, y0=40, x1=60,  y1=60, line=dict(color=BLANCO, width=1), layer="below")
    fig.add_shape(type="rect",   x0=20, y0=0,  x1=80,  y1=16, line=dict(color=BLANCO, width=1), fillcolor="rgba(0,0,0,0)", layer="below")
    fig.add_shape(type="rect",   x0=20, y0=84, x1=80,  y1=100,line=dict(color=BLANCO, width=1), fillcolor="rgba(0,0,0,0)", layer="below")
    fig.add_shape(type="rect",   x0=35, y0=0,  x1=65,  y1=6,  line=dict(color=BLANCO, width=1), fillcolor="rgba(0,0,0,0)", layer="below")
    fig.add_shape(type="rect",   x0=35, y0=94, x1=65,  y1=100,line=dict(color=BLANCO, width=1), fillcolor="rgba(0,0,0,0)", layer="below")

    # Claves en valores REALES del DB (sin tilde) + con tilde como fallback
    COLOR_NIVEL = {
        "Elite":    "#FFD700",   # dorado
        "Bueno":    "#00BFFF",   # azul
        "Regular":  "#90EE90",   # verde
        "Bajo":     "#FFA07A",   # salmón
        # Con tilde por compatibilidad
        "Élite":    "#FFD700",
        "Alto":     "#00BFFF",
        "Medio":    "#90EE90",
        "Básico":   "#FFA07A",
    }

    for _, row in df.iterrows():
        x     = float(row.get("x_coord", 50))
        y     = float(row.get("y_coord", 50))
        name  = str(row.get("jugador", "")).title()
        score = float(row.get("score_100", 0))
        nivel = str(row.get("nivel", ""))
        equipo= str(row.get("nombre_equipo", "")).title()
        pos   = str(row.get("posicion_xi_nombre", ""))
        goles = int(row.get("goles", 0))
        asis  = int(row.get("asistencias", 0))
        color = COLOR_NIVEL.get(nivel, "#FFD700")

        # Burbuja grande de color (ring visible alrededor de la foto)
        fig.add_trace(go.Scatter(
            x=[x], y=[y],
            mode="markers",
            marker=dict(size=52, color=color,
                        line=dict(color="white", width=2.5)),
            customdata=[[name, equipo, pos, goles, asis, nivel, score]],
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "%{customdata[2]} · %{customdata[1]}<br>"
                "Score: %{customdata[6]:.0f}<br>"
                "Goles: %{customdata[3]}  Asist: %{customdata[4]}<br>"
                "Nivel: %{customdata[5]}"
                "<extra></extra>"
            ),
            showlegend=False,
        ))

        # Foto encima (más pequeña que la burbuja → ring de color visible alrededor)
        foto_url = str(row.get("foto_url", ""))
        foto_b64 = _load_foto_cached(foto_url) if foto_url else None
        if foto_b64:
            fig.add_layout_image(dict(
                source=f"data:image/jpeg;base64,{foto_b64}",
                x=x, y=y,
                xref="x", yref="y",
                sizex=7, sizey=7,          # ~30px en campo de 430px; burbuja es 52px
                xanchor="center", yanchor="middle",
                layer="above",
                opacity=0.95,
            ))
        else:
            # Sin foto: mostrar score dentro de la burbuja
            fig.add_annotation(
                x=x, y=y,
                text=f"<b>{score:.0f}</b>",
                showarrow=False,
                font=dict(color="black", size=10, family="Arial Bold"),
            )

        # Score y apellido bajo la burbuja
        apellido = name.split()[-1] if name else ""
        fig.add_annotation(
            x=x, y=y - 9,
            text=f"<b>{apellido[:12]}</b>",
            showarrow=False,
            font=dict(color="white", size=8, family="Arial"),
            bgcolor="rgba(0,0,0,0.65)",
            borderpad=1,
        )

    fig.update_layout(
        width=430, height=590,
        xaxis=dict(range=[0, 100], showgrid=False, zeroline=False,
                   showticklabels=False, fixedrange=True),
        yaxis=dict(range=[0, 100], showgrid=False, zeroline=False,
                   showticklabels=False, fixedrange=True),
        margin=dict(l=5, r=5, t=30, b=5),
        plot_bgcolor=VERDE,
        paper_bgcolor="#1a1a2e",
        font_color="white",
        title=dict(text="⚽ XI Ideal Liga 1 · Score ML", x=0.5,
                   font=dict(size=13)),
    )

    # Leyenda de colores (con valores reales del DB)
    leyenda_niveles = [
        ("Elite",   "#FFD700"),
        ("Bueno",   "#00BFFF"),
        ("Regular", "#90EE90"),
        ("Bajo",    "#FFA07A"),
    ]
    leyenda_cols = st.columns(4)
    for i, (niv, col) in enumerate(leyenda_niveles):
        with leyenda_cols[i]:
            st.markdown(
                f'<span style="background:{col};border-radius:50%;'
                f'display:inline-block;width:12px;height:12px;"></span> {niv}',
                unsafe_allow_html=True,
            )

    st.plotly_chart(fig, use_container_width=False)


# ─── Renderizado principal de datos ──────────────────────────────────────────

def _render_data(df, pregunta: str, chart_type_hint: str | None):
    """Muestra campo XI, tabla y/o gráfico según lo que el usuario pidió."""
    if df is None or df.empty:
        return

    mostrar_tabla        = _pide_tabla(pregunta)
    mostrar_graf, tipo_g = _pide_grafico(pregunta)
    es_xi                = (_es_xi_ideal(pregunta) or chart_type_hint == "xi") and "x_coord" in df.columns

    # XI Ideal: SIEMPRE muestra el campo de fútbol (no necesita que el usuario pida gráfico)
    if es_xi:
        _render_xi_campo(df)

    # ── Tabla ────────────────────────────────────────────────────────────────
    label = "📋 Ver jugadores del XI" if es_xi else ("📋 Datos detallados" if mostrar_tabla else "📋 Ver datos")
    with st.expander(label, expanded=(mostrar_tabla and not es_xi)):
        df_display = df.copy().reset_index(drop=True)
        df_display.index = df_display.index + 1
        cols_ocultar = ["x_coord", "y_coord", "foto_url", "slot", "orden",
                        "nivel_num", "alias_equipo"]
        df_display = df_display.drop(
            columns=[c for c in cols_ocultar if c in df_display.columns]
        )
        # Evitar "2,025" — mostrar año como texto sin separador de miles
        for col in ["temporada", "periodo"]:
            if col in df_display.columns:
                df_display[col] = df_display[col].astype(str)
        st.dataframe(df_display, use_container_width=True)

    # ── Butterfly chart VS — SIEMPRE visible cuando hay dos entidades comparadas ──
    es_vs_jugadores  = chart_type_hint == "radar" and "jugador" in df.columns and df["jugador"].nunique() >= 2
    es_vs_equipos    = chart_type_hint == "bar_vs" and "nombre_equipo" in df.columns and df["nombre_equipo"].nunique() >= 2
    es_vs_entrenador = chart_type_hint == "bar_vs" and "entrenador" in df.columns and df["entrenador"].nunique() >= 2

    if es_vs_jugadores:
        _render_butterfly_vs(df, "jugador")
    elif es_vs_equipos:
        _render_butterfly_vs(df, "nombre_equipo")
    elif es_vs_entrenador:
        _render_butterfly_vs(df, "entrenador")

    # Gráfico convencional (bar, line, radar): SOLO si el usuario lo pide explícitamente
    if mostrar_graf:
        tipo = tipo_g if tipo_g else (chart_type_hint or "bar")
        if tipo in ("bar_vs",) and not (es_vs_equipos or es_vs_entrenador):
            _render_bar_vs(df)       # fallback si butterfly no aplica
        elif tipo == "radar" and not es_vs_jugadores:
            _render_radar(df)
        elif tipo == "radar" and es_vs_jugadores:
            _render_radar(df)        # radar adicional si el usuario lo pidió
        else:
            _render_grafico(df, tipo)


# ─── Historial ────────────────────────────────────────────────────────────────

if "mensajes" not in st.session_state:
    st.session_state.mensajes = []

for msg in st.session_state.mensajes:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg.get("df") is not None:
            _render_data(msg["df"], msg.get("pregunta", ""), msg.get("chart_type"))

# ─── Input ────────────────────────────────────────────────────────────────────

pregunta_default = st.session_state.pop("pregunta_rapida", None)
pregunta = st.chat_input("Escribe tu pregunta sobre Liga 1...")
if pregunta_default and not pregunta:
    pregunta = pregunta_default

if pregunta:
    st.session_state.mensajes.append({"role": "user", "content": pregunta})
    with st.chat_message("user"):
        st.markdown(pregunta)

    with st.chat_message("assistant"):
        with st.spinner("Consultando datos..."):
            try:
                respuesta, df, chart_type = responder(pregunta)
                st.markdown(respuesta)
                if df is not None and not df.empty:
                    _render_data(df, pregunta, chart_type)
                st.session_state.mensajes.append({
                    "role": "assistant",
                    "content": respuesta,
                    "df": df,
                    "chart_type": chart_type,
                    "pregunta": pregunta,
                })
            except Exception as e:
                error_msg = f"❌ Error: {e}"
                st.error(error_msg)
                st.session_state.mensajes.append({
                    "role": "assistant", "content": error_msg,
                    "df": None, "chart_type": None, "pregunta": pregunta,
                })

# ─── Footer ───────────────────────────────────────────────────────────────────

st.caption("Liga 1 Perú · Azure OpenAI gpt-5.4-mini + Databricks Delta Lake")
