import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import date

# 1. PAGE CONFIG 

st.set_page_config(
    page_title="Northwind BI Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


st.markdown("""
<style>
/* Global */
html, body, [class*="css"]  {
    font-family: 'Inter', sans-serif;
}

/* Header */
.main-title {
    font-size: 2.2rem;
    font-weight: 700;
    color: #1f2937;
}
.subtitle {
    color: #6b7280;
    margin-bottom: 1.5rem;
}

/* KPI Cards */
.kpi-card {
    background: white;
    border-radius: 14px;
    padding: 18px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.06);
    text-align: center;
}
.kpi-title {
    font-size: 0.85rem;
    color: #6b7280;
}
.kpi-value {
    font-size: 1.8rem;
    font-weight: 500;
    color: #111827;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background-color: #f9fafb;
}
</style>
""", unsafe_allow_html=True)
# 3. LOAD DATA 
PROCESSED_FILE = "data/processed/Northwind_Final_Analytics.csv"

@st.cache_data
def load_data():
    df = pd.read_csv(PROCESSED_FILE)
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
    df = df.dropna(subset=['OrderDate'])
    return df

try:
    df = load_data()
except:
    st.error("❌ Impossible de charger les données.")
    st.stop()


# 4. SIDEBAR

with st.sidebar:
    st.title(" Filtres")

    min_date = df['OrderDate'].min().date()
    max_date = df['OrderDate'].max().date()

    date_range = st.date_input(
        " Période",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    status_list = ['Tous'] + sorted(df['Statut'].unique())
    selected_status = st.selectbox(" Statut de livraison", status_list)

   

mask = (df['OrderDate'].dt.date >= date_range[0]) & (df['OrderDate'].dt.date <= date_range[1])
if selected_status != 'Tous':
    mask &= df['Statut'] == selected_status

df_filtered = df.loc[mask]


# 6. HEADER

st.markdown("<div class='main-title'>Northwind Business Intelligence</div>", unsafe_allow_html=True)
st.markdown(
    f"<div class='subtitle'>Analyse des commandes du {date_range[0].strftime('%d/%m/%Y')} au {date_range[1].strftime('%d/%m/%Y')}</div>",
    unsafe_allow_html=True
)

# 7. KPI SECTION 

total_orders = len(df_filtered)
livre_count = df_filtered[df_filtered['Statut'] == 'Livré'].shape[0]
non_livre_count = total_orders - livre_count
pct_livre = (livre_count / total_orders * 100) if total_orders else 0
last_year = df_filtered['OrderDate'].max().year if total_orders else "—"

k1, k2, k3, k4 = st.columns(4)

with k1:
    st.markdown(f"""
    <div class='kpi-card'>
        <div class='kpi-title'>Total commandes</div>
        <div class='kpi-value'>{total_orders}</div>
    </div>
    """, unsafe_allow_html=True)

with k2:
    st.markdown(f"""
    <div class='kpi-card'>
        <div class='kpi-title'>Commandes livrées</div>
        <div class='kpi-value'>{livre_count}</div>
    </div>
    """, unsafe_allow_html=True)

with k3:
    st.markdown(f"""
    <div class='kpi-card'>
        <div class='kpi-title'>Non livrées</div>
        <div class='kpi-value'>{non_livre_count}</div>
    </div>
    """, unsafe_allow_html=True)

with k4:
    st.markdown(f"""
    <div class='kpi-card'>
        <div class='kpi-title'>Dernière activité</div>
        <div class='kpi-value'>{last_year}</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("")
    st.markdown("")



    mask = (
    (df['OrderDate'].dt.date >= date_range[0]) &
    (df['OrderDate'].dt.date <= date_range[1])
    )

    if selected_status != 'Tous':
        mask &= df['Statut'] == selected_status

    df_filtered = df.loc[mask]
    st.download_button(
        label="⬇️ Exporter les données filtrées (CSV)",
        data=df_filtered.to_csv(index=False).encode("utf-8"),
        file_name="northwind_export.csv",
        mime="text/csv"
    )

# 8. TABS – COGNITIVE LOAD REDUCTION

tab1, tab2, tab3 = st.tabs([
    " Vue d'ensemble",
    " Clients & Employés",
    " Analyse OLAP 3D"
])

# ---------- TAB 1 ----------
with tab1:
    c1, c2 = st.columns([1, 2])

    with c1:
        fig_pie = px.pie(
            df_filtered,
            names='Statut',
            hole=0.45,
            color='Statut',
            color_discrete_map={'Livré': '#10b981', 'Non Livré': '#ef4444'}
        )
        fig_pie.update_layout(title="Répartition des livraisons")
        st.plotly_chart(fig_pie, use_container_width=True)

    with c2:
        df_time = df_filtered.groupby([
            pd.Grouper(key='OrderDate', freq='M'), 'Statut'
        ]).size().reset_index(name='Volume')

        fig_line = px.line(
            df_time,
            x='OrderDate',
            y='Volume',
            color='Statut',
            markers=True,
            title="Évolution mensuelle"
        )
        st.plotly_chart(fig_line, use_container_width=True)

# ---------- TAB 2 ----------
with tab2:
    c1, c2 = st.columns(2)

    with c1:
        top_clients = df_filtered['CompanyName'].value_counts().nlargest(10).index
        df_top = df_filtered[df_filtered['CompanyName'].isin(top_clients)]
        fig_clients = px.histogram(
            df_top,
            y='CompanyName',
            color='Statut',
            barmode='stack',
            title="Top 10 clients"
        )
        st.plotly_chart(fig_clients, use_container_width=True)

    with c2:
        fig_emp = px.histogram(
            df_filtered,
            x='EmployeeName',
            color='Statut',
            barmode='group',
            title="Performance des employés"
        )
        st.plotly_chart(fig_emp, use_container_width=True)

# ---------- TAB 3 ----------
with tab3:
    st.info("💡 Analyse multidimensionnelle : Date × Client × Employé")

    if total_orders:
        df_3d = df_filtered.groupby([
            'OrderDate', 'CompanyName', 'EmployeeName', 'Statut'
        ]).size().reset_index(name='Volume')

        fig_3d = px.scatter_3d(
            df_3d,
            x='OrderDate',
            y='CompanyName',
            z='EmployeeName',
            size='Volume',
            color='Statut',
            opacity=0.85,
            height=720,
            color_discrete_map={
                'Livré': '#16a34a',      
                'Non Livré': '#dc2626' 
            }
        )
        st.plotly_chart(fig_3d, use_container_width=True)
    else:
        st.warning("Aucune donnée disponible")


# 9. DATA TABLE (SECONDARY)

with st.expander("📄 Afficher les données brutes"):
    st.dataframe(df_filtered, use_container_width=True)





