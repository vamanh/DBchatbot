"""
RegBot — Smart Regulatory Reporting Analyst
Streamlit Web UI wrapper around the chatbot logic
"""

import os
import json
import pandas as pd
import streamlit as st
from openai import OpenAI
from supabase import create_client, Client
from dotenv import load_dotenv
import warnings

warnings.filterwarnings("ignore")
load_dotenv(override=True)

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="RegBot — Regulatory Reporting Analyst",
    page_icon="📊",
    layout="wide",
)

# ── Clients (lazy, keyed by credentials so cache refreshes on change) ─────────
@st.cache_resource
def get_clients(openai_key: str, sb_url: str, sb_key: str):
    oa = OpenAI(api_key=openai_key)
    sb = create_client(sb_url, sb_key)
    return oa, sb

MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Credentials — prefer .env, fall back to session_state (entered via UI)
def _cred(env_key: str, state_key: str) -> str:
    return os.getenv(env_key) or st.session_state.get(state_key, "")

# ── Sidebar credential form (shown when .env is missing) ──────────────────────
_missing_creds = not (os.getenv("OPENAI_API_KEY") and os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_KEY"))

with st.sidebar:
    st.title("📊 RegBot")
    st.caption("Regulatory Reporting Analyst")
    if _missing_creds:
        st.warning("No `.env` found — enter credentials below.")
        with st.form("creds_form"):
            st.session_state["_oai_key"] = st.text_input(
                "OpenAI API Key", value=st.session_state.get("_oai_key",""), type="password")
            st.session_state["_sb_url"]  = st.text_input(
                "Supabase URL",  value=st.session_state.get("_sb_url",""))
            st.session_state["_sb_key"]  = st.text_input(
                "Supabase Anon Key", value=st.session_state.get("_sb_key",""), type="password")
            st.session_state["_model"]   = st.text_input(
                "OpenAI Model", value=st.session_state.get("_model", "gpt-4o-mini"))
            if st.form_submit_button("💾 Save & Connect", use_container_width=True):
                get_clients.clear()
                st.rerun()
        st.divider()

_oai = _cred("OPENAI_API_KEY", "_oai_key")
_sb_url = _cred("SUPABASE_URL",  "_sb_url")
_sb_key = _cred("SUPABASE_KEY",  "_sb_key")
MODEL   = os.getenv("OPENAI_MODEL") or st.session_state.get("_model", "gpt-4o-mini")

if not (_oai and _sb_url and _sb_key):
    st.info("👈 Enter your credentials in the sidebar to get started.")
    st.stop()

openai_client, supabase = get_clients(_oai, _sb_url, _sb_key)

SYSTEM_PROMPT = """You are RegBot, a smart regulatory reporting data analyst assistant.
You help financial institutions reconcile ledger and risk datasets, identify breaks,
and flag regulatory risks. Be concise, professional, and always highlight the regulatory
implications of any discrepancies you find."""

COMMON_COLUMNS = ["account_id", "counterparty_id", "currency", "amount", "region", "legal_entity"]

# ── Session state init ────────────────────────────────────────────────────────
DEFAULTS = {
    "phase"      : "initial",
    "messages"   : [],
    "ledger_df"  : None,
    "risk_df"    : None,
    "key_attrs"  : [],
    "agg_attrs"  : [],
    "recon_df"   : None,
    "llm_history": [{"role": "system", "content": SYSTEM_PROMPT}],
}
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── LLM helpers ───────────────────────────────────────────────────────────────
def llm(messages: list, json_mode: bool = False) -> str:
    kwargs = {"model": MODEL, "messages": messages, "temperature": 0.3}
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}
    try:
        return openai_client.chat.completions.create(**kwargs).choices[0].message.content.strip()
    except Exception as e:
        err = str(e)
        if "401" in err or "invalid_api_key" in err or "AuthenticationError" in err:
            st.error("❌ **Invalid OpenAI API key.** Please update `OPENAI_API_KEY` in your `.env` file and restart the server.")
            st.stop()
        raise

def detect_intent(user_msg: str) -> str:
    prompt = f"""Classify this message into ONE tag:
FETCH_DATA    – user wants to load/fetch/pull the datasets
RECON_YES     – user agrees to run reconciliation
RECON_NO      – user declines reconciliation
EXPORT        – user wants to export/save/download results
SHOW_BREAKS   – user wants to see only breaks/discrepancies
SHOW_SUMMARY  – user wants a summary or overview
QUIT          – user wants to exit/quit/bye
OTHER         – anything else

Message: "{user_msg}"
Return JSON: {{"intent": "<TAG>"}}"""
    raw = llm([{"role": "user", "content": prompt}], json_mode=True)
    return json.loads(raw).get("intent", "OTHER")

def extract_attributes(user_msg: str, available: list, attr_type: str) -> list:
    prompt = f"""The user is specifying {attr_type} for a reconciliation.
Available columns: {available}
User said: "{user_msg}"
Return only column names that exist in the available list.
JSON: {{"attributes": ["col1", ...]}}"""
    raw = llm([{"role": "user", "content": prompt}], json_mode=True)
    attrs = json.loads(raw).get("attributes", [])
    return [a for a in attrs if a in available]

def ai_insights(recon_df: pd.DataFrame, key_attrs: list, agg_attrs: list) -> str:
    counts = recon_df["recon_status"].value_counts().to_dict()
    summary = {
        "total": len(recon_df), **counts,
        "key_attributes": key_attrs, "agg_attributes": agg_attrs,
    }
    for col in agg_attrs:
        if f"{col}_diff" in recon_df.columns:
            summary[f"total_{col}_difference"] = float(
                recon_df[recon_df["recon_status"] == "AMOUNT_BREAK"][f"{col}_diff"].abs().sum()
            )
    msgs = st.session_state.llm_history + [{
        "role": "user",
        "content": f"Recon summary: {json.dumps(summary)}. Give 3-4 bullet-point regulatory insights. Be concise.",
    }]
    return llm(msgs)

# ── Data helpers ──────────────────────────────────────────────────────────────
def fetch_data() -> tuple[bool, str]:
    try:
        ledger_rows = supabase.table("ledger_balance").select("*").execute().data
        risk_rows   = supabase.table("risk_table").select("*").execute().data
        st.session_state.ledger_df = pd.DataFrame(ledger_rows)
        st.session_state.risk_df   = pd.DataFrame(risk_rows)
        return True, ""
    except Exception as e:
        return False, str(e)

def perform_recon(ledger_df, risk_df, key_attrs, agg_attrs) -> pd.DataFrame:
    la = ledger_df.groupby(key_attrs)[agg_attrs].sum().reset_index()
    ra = risk_df.groupby(key_attrs)[agg_attrs].sum().reset_index()
    la = la.rename(columns={c: f"ledger_{c}" for c in agg_attrs})
    ra = ra.rename(columns={c: f"risk_{c}"   for c in agg_attrs})
    merged = pd.merge(la, ra, on=key_attrs, how="outer", indicator=True)
    merged["recon_status"] = "MATCHED"
    merged.loc[merged["_merge"] == "left_only",  "recon_status"] = "LEDGER_ONLY"
    merged.loc[merged["_merge"] == "right_only", "recon_status"] = "RISK_ONLY"
    for col in agg_attrs:
        merged[f"{col}_diff"] = merged[f"ledger_{col}"].fillna(0) - merged[f"risk_{col}"].fillna(0)
        mask = (merged["recon_status"] == "MATCHED") & (merged[f"{col}_diff"].abs() > 0.01)
        merged.loc[mask, "recon_status"] = "AMOUNT_BREAK"
    return merged.drop(columns=["_merge"])

STATUS_COLORS = {
    "MATCHED"     : "background-color: #d4edda; color: #155724",
    "AMOUNT_BREAK": "background-color: #fff3cd; color: #856404",
    "LEDGER_ONLY" : "background-color: #f8d7da; color: #721c24",
    "RISK_ONLY"   : "background-color: #e8d5f5; color: #6f42c1",
}

def style_recon(df: pd.DataFrame) -> pd.DataFrame.style:
    def row_color(row):
        color = STATUS_COLORS.get(row["recon_status"], "")
        return [color] * len(row)
    return df.style.apply(row_color, axis=1)

# ── Sidebar — data stats (appended to the credential sidebar block above) ─────
with st.sidebar:
    st.divider()
    if st.session_state.ledger_df is not None:
        ldf = st.session_state.ledger_df
        rdf = st.session_state.risk_df
        st.markdown("**Ledger Balance**")
        st.metric("Rows", len(ldf))
        st.metric("Total Amount", f"{ldf['amount'].sum():,.0f}")
        st.markdown("**Risk Table**")
        st.metric("Rows", len(rdf))
        st.metric("Total Amount", f"{rdf['amount'].sum():,.0f}")
        st.divider()

    if st.session_state.recon_df is not None:
        rdf = st.session_state.recon_df
        counts = rdf["recon_status"].value_counts()
        st.markdown("**Recon Status**")
        for status, color_hex in [("MATCHED","#28a745"),("AMOUNT_BREAK","#ffc107"),
                                   ("LEDGER_ONLY","#dc3545"),("RISK_ONLY","#6f42c1")]:
            n = counts.get(status, 0)
            st.markdown(f'<span style="color:{color_hex}">■</span> **{status}**: {n}', unsafe_allow_html=True)
        st.divider()

    if st.button("🔄 Reset Session", use_container_width=True):
        for k, v in DEFAULTS.items():
            st.session_state[k] = v
        st.rerun()

# ── Main header ───────────────────────────────────────────────────────────────
st.title("📊 RegBot — Regulatory Reporting Analyst")
st.caption("Powered by OpenAI + Supabase · Smart data analyst for regulatory reporting")
st.divider()

# ── Render chat history ───────────────────────────────────────────────────────
for msg in st.session_state.messages:
    with st.chat_message(msg["role"], avatar="🤖" if msg["role"] == "assistant" else "👤"):
        st.markdown(msg["content"])

# Initial greeting
if not st.session_state.messages:
    greeting = "👋 **What do you want to do today?**\n\nI can help you:\n- **Fetch** the ledger and risk datasets from Supabase\n- **Reconcile** them across key attributes\n- **Identify** regulatory breaks and discrepancies"
    with st.chat_message("assistant", avatar="🤖"):
        st.markdown(greeting)
    st.session_state.messages.append({"role": "assistant", "content": greeting})

# ── Data display (shown once after fetch) ────────────────────────────────────
if st.session_state.phase in ("recon_confirm", "key_attrs", "agg_attrs", "done") and \
        st.session_state.ledger_df is not None:
    with st.expander("📋 Ledger Balance Dataset", expanded=False):
        st.dataframe(st.session_state.ledger_df, use_container_width=True, height=250)
    with st.expander("📋 Risk Table Dataset", expanded=False):
        st.dataframe(st.session_state.risk_df, use_container_width=True, height=250)

# ── Recon display ─────────────────────────────────────────────────────────────
if st.session_state.recon_df is not None:
    with st.expander("🔍 Reconciliation Results", expanded=True):
        tabs = st.tabs(["All Records", "Breaks Only", "Matched Only"])
        rdf = st.session_state.recon_df

        with tabs[0]:
            st.dataframe(style_recon(rdf), use_container_width=True, height=300)
        with tabs[1]:
            breaks = rdf[rdf["recon_status"] != "MATCHED"]
            if breaks.empty:
                st.success("No breaks found — datasets are fully reconciled!")
            else:
                st.dataframe(style_recon(breaks), use_container_width=True, height=300)
        with tabs[2]:
            matched = rdf[rdf["recon_status"] == "MATCHED"]
            st.dataframe(matched, use_container_width=True, height=300)

        # Download button
        csv = rdf.to_csv(index=False).encode()
        st.download_button("⬇️ Download Recon Results (CSV)", csv, "recon_output.csv", "text/csv")

# ── Chat input & processing ───────────────────────────────────────────────────
def add_bot(text: str):
    st.session_state.messages.append({"role": "assistant", "content": text})
    st.session_state.llm_history.append({"role": "assistant", "content": text})

def add_user(text: str):
    st.session_state.llm_history.append({"role": "user", "content": text})

if prompt := st.chat_input("Type your message…"):
    with st.chat_message("user", avatar="👤"):
        st.markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})
    add_user(prompt)

    phase   = st.session_state.phase
    intent  = detect_intent(prompt)

    # ── QUIT ──────────────────────────────────────────────────────────────────
    if intent == "QUIT":
        add_bot("👋 Goodbye! Stay compliant.")

    # ── INITIAL / FETCHED ─────────────────────────────────────────────────────
    elif phase in ("initial", "fetched"):
        if intent == "FETCH_DATA":
            with st.spinner("Fetching data from Supabase…"):
                ok, err = fetch_data()
            if ok:
                ldf = st.session_state.ledger_df
                rdf = st.session_state.risk_df
                reply = (
                    f"✅ **Data loaded successfully!**\n\n"
                    f"| Dataset | Rows | Total Amount |\n|---|---|---|\n"
                    f"| Ledger Balance | {len(ldf)} | {ldf['amount'].sum():,.2f} |\n"
                    f"| Risk Table     | {len(rdf)} | {rdf['amount'].sum():,.2f} |\n\n"
                    f"Common columns available: `{'`, `'.join(COMMON_COLUMNS)}`\n\n"
                    "**Should I run a reconciliation between the two datasets?**"
                )
                add_bot(reply)
                st.session_state.phase = "recon_confirm"
            else:
                add_bot(f"❌ Failed to fetch data: `{err}`\n\nCheck your `.env` credentials.")
        else:
            reply = llm(st.session_state.llm_history)
            add_bot(reply)

    # ── RECON CONFIRM ─────────────────────────────────────────────────────────
    elif phase == "recon_confirm":
        if intent == "RECON_YES":
            reply = (
                "Great! Let's set up the reconciliation.\n\n"
                f"Available columns: `{'`, `'.join(COMMON_COLUMNS)}`\n\n"
                "**Step 1:** Which columns should I use as **key attributes** to join on?\n"
                "_(e.g. 'account_id and currency')_"
            )
            add_bot(reply)
            st.session_state.phase = "key_attrs"
        elif intent == "RECON_NO":
            add_bot("No problem. You can ask me anything about the data, or say **fetch** to reload.")
            st.session_state.phase = "fetched"
        else:
            reply = llm(st.session_state.llm_history)
            add_bot(reply)

    # ── KEY ATTRIBUTES ────────────────────────────────────────────────────────
    elif phase == "key_attrs":
        attrs = extract_attributes(prompt, COMMON_COLUMNS, "key/join attributes")
        if not attrs:
            add_bot(f"I couldn't identify valid key attributes from that. Available: `{'`, `'.join(COMMON_COLUMNS)}`\n\nPlease try again.")
        else:
            st.session_state.key_attrs = attrs
            reply = (
                f"✅ Key attributes set: **{attrs}**\n\n"
                "**Step 2:** Which columns should I **aggregate** (sum) for comparison?\n"
                "_(e.g. 'amount')_"
            )
            add_bot(reply)
            st.session_state.phase = "agg_attrs"

    # ── AGG ATTRIBUTES → RUN RECON ────────────────────────────────────────────
    elif phase == "agg_attrs":
        numeric_cols = ["amount"]
        attrs = extract_attributes(prompt, numeric_cols, "aggregate/sum attributes")
        if not attrs:
            add_bot(f"I couldn't identify valid aggregate attributes. Available: `{'`, `'.join(numeric_cols)}`\n\nPlease try again.")
        else:
            st.session_state.agg_attrs = attrs
            with st.spinner("Running reconciliation…"):
                recon_df = perform_recon(
                    st.session_state.ledger_df, st.session_state.risk_df,
                    st.session_state.key_attrs, attrs
                )
                st.session_state.recon_df = recon_df
                insights = ai_insights(recon_df, st.session_state.key_attrs, attrs)

            counts = recon_df["recon_status"].value_counts()
            total_diff = recon_df[[c for c in recon_df.columns if c.endswith("_diff")]].abs().sum().sum()
            reply = (
                f"✅ **Reconciliation complete!**\n\n"
                f"| Status | Count |\n|---|---|\n"
                f"| 🟢 MATCHED | {counts.get('MATCHED', 0)} |\n"
                f"| 🟡 AMOUNT_BREAK | {counts.get('AMOUNT_BREAK', 0)} |\n"
                f"| 🔴 LEDGER_ONLY | {counts.get('LEDGER_ONLY', 0)} |\n"
                f"| 🟣 RISK_ONLY | {counts.get('RISK_ONLY', 0)} |\n\n"
                f"**Total amount discrepancy:** `{total_diff:,.2f}`\n\n"
                f"---\n**📋 Regulatory Insights:**\n{insights}"
            )
            add_bot(reply)
            st.session_state.phase = "done"

    # ── DONE — follow-up queries ───────────────────────────────────────────────
    elif phase == "done":
        if intent in ("SHOW_BREAKS", "SHOW_SUMMARY"):
            counts = st.session_state.recon_df["recon_status"].value_counts()
            breaks = st.session_state.recon_df[st.session_state.recon_df["recon_status"] != "MATCHED"]
            add_bot(
                f"There are **{len(breaks)} break records** out of {len(st.session_state.recon_df)} total.\n"
                "See the **Breaks Only** tab in the Reconciliation Results panel above."
            )
        elif "new recon" in prompt.lower() or "another recon" in prompt.lower():
            st.session_state.recon_df = None
            add_bot("Sure! Let's run another recon.\n\n**Key attributes** — which columns to join on?")
            st.session_state.phase = "key_attrs"
        else:
            context = (
                f"The user has completed a recon. Status counts: "
                f"{st.session_state.recon_df['recon_status'].value_counts().to_dict()}. "
                f"Key attrs: {st.session_state.key_attrs}, Agg attrs: {st.session_state.agg_attrs}."
            )
            reply = llm(st.session_state.llm_history + [{"role": "user", "content": f"{context}\nUser: {prompt}"}])
            add_bot(reply)

    st.rerun()
