"""
RegBot — Smart Regulatory Reporting Analyst
Conversational chatbot: OpenAI LLM + Supabase data + Pandas recon engine
"""

import os
import json
import sys
import pandas as pd
from openai import OpenAI
from supabase import create_client, Client
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box
import warnings

warnings.filterwarnings("ignore")
load_dotenv()

# ── Clients ───────────────────────────────────────────────────────────────────
console = Console()

def _require_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        console.print(f"[bold red]Missing env var: {key}. Check your .env file.[/]")
        sys.exit(1)
    return val

openai_client = OpenAI(api_key=_require_env("OPENAI_API_KEY"))
supabase: Client = create_client(_require_env("SUPABASE_URL"), _require_env("SUPABASE_KEY"))
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# ── System Prompt ─────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are RegBot, a smart regulatory reporting data analyst assistant.
You help financial institutions reconcile ledger and risk datasets, identify breaks,
and flag regulatory risk. You are concise, professional, and always highlight the
regulatory implications of any discrepancies you find.
When summarising data or recon results, keep responses brief but insightful."""

# ── Session State ─────────────────────────────────────────────────────────────
session = {
    "phase": "initial",           # initial → fetched → recon_confirm → key_attrs → agg_attrs → done
    "ledger_df": None,
    "risk_df": None,
    "key_attrs": [],
    "agg_attrs": [],
    "recon_df": None,
    "history": [{"role": "system", "content": SYSTEM_PROMPT}],
}

COMMON_COLUMNS = ["account_id", "counterparty_id", "currency", "amount", "region", "legal_entity"]

# ── Display Helpers ───────────────────────────────────────────────────────────
def bot(msg: str):
    console.print(f"\n[bold cyan]RegBot:[/] {msg}")

def user_prompt() -> str:
    console.print()
    return console.input("[bold green]You:[/] ").strip()

def show_df(df: pd.DataFrame, title: str, max_rows: int = 10):
    table = Table(title=title, box=box.ROUNDED, style="dim", header_style="bold magenta")
    for col in df.columns:
        table.add_column(str(col), overflow="fold")
    for _, row in df.head(max_rows).iterrows():
        table.add_row(*[str(v) if pd.notna(v) else "—" for v in row])
    if len(df) > max_rows:
        table.caption = f"… {len(df) - max_rows} more rows"
    console.print(table)

def show_summary(df: pd.DataFrame, name: str):
    numeric = df.select_dtypes("number")
    console.print(Panel(
        f"[bold]{name}[/]\n"
        f"  Rows      : [yellow]{len(df):,}[/]\n"
        f"  Columns   : {list(df.columns)}\n"
        f"  Currencies: {sorted(df['currency'].unique().tolist()) if 'currency' in df.columns else 'n/a'}\n"
        f"  Regions   : {sorted(df['region'].unique().tolist()) if 'region' in df.columns else 'n/a'}\n"
        f"  Total Amount: [green]{numeric['amount'].sum():,.2f}[/]" if 'amount' in numeric.columns else "",
        title=f"[bold blue]{name} Summary[/]",
        border_style="blue",
    ))

# ── OpenAI Helpers ────────────────────────────────────────────────────────────
def llm(messages: list, json_mode: bool = False) -> str:
    kwargs = {"model": MODEL, "messages": messages, "temperature": 0.3}
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}
    resp = openai_client.chat.completions.create(**kwargs)
    return resp.choices[0].message.content.strip()

def detect_intent(user_msg: str) -> str:
    """Classify user intent into a known action tag."""
    prompt = f"""Classify the user message into ONE of these tags:
FETCH_DATA       – user wants to load / fetch / pull the datasets
RECON_YES        – user agrees to run reconciliation (yes, sure, go ahead, do it, proceed)
RECON_NO         – user declines reconciliation
EXPORT           – user wants to export or save results
SHOW_BREAKS      – user wants to see only the breaks / discrepancies
SHOW_SUMMARY     – user wants a summary or overview
QUIT             – user wants to exit / quit / bye
OTHER            – anything else

User message: "{user_msg}"
Return JSON: {{"intent": "<TAG>", "confidence": 0.0}}"""
    raw = llm([{"role": "user", "content": prompt}], json_mode=True)
    return json.loads(raw).get("intent", "OTHER")

def extract_attributes(user_msg: str, available: list, attr_type: str) -> list:
    """Extract column names from a natural language message."""
    prompt = f"""The user is specifying {attr_type} for a reconciliation.
Available columns: {available}
User said: "{user_msg}"
Return the column names (exact match from available list) the user intends.
JSON: {{"attributes": ["col1", ...]}}"""
    raw = llm([{"role": "user", "content": prompt}], json_mode=True)
    attrs = json.loads(raw).get("attributes", [])
    # safety: keep only valid columns
    return [a for a in attrs if a in available]

def ai_insights(recon_df: pd.DataFrame, key_attrs: list, agg_attrs: list) -> str:
    """Ask OpenAI to generate regulatory insights from recon summary."""
    summary = {
        "total_records"   : len(recon_df),
        "matched"         : int((recon_df["recon_status"] == "MATCHED").sum()),
        "amount_breaks"   : int((recon_df["recon_status"] == "AMOUNT_BREAK").sum()),
        "ledger_only"     : int((recon_df["recon_status"] == "LEDGER_ONLY").sum()),
        "risk_only"       : int((recon_df["recon_status"] == "RISK_ONLY").sum()),
        "key_attributes"  : key_attrs,
        "agg_attributes"  : agg_attrs,
    }
    for col in agg_attrs:
        breaks = recon_df[recon_df["recon_status"] == "AMOUNT_BREAK"]
        if f"{col}_diff" in breaks.columns:
            summary[f"total_{col}_difference"] = float(breaks[f"{col}_diff"].abs().sum())
    messages = session["history"] + [{
        "role": "user",
        "content": f"Here is the reconciliation summary: {json.dumps(summary)}. "
                   "Provide 3-4 bullet-point regulatory insights. Be concise."
    }]
    return llm(messages)

# ── Data Layer ────────────────────────────────────────────────────────────────
def fetch_data() -> bool:
    try:
        bot("Fetching ledger balance data from Supabase…")
        ledger_rows = supabase.table("ledger_balance").select("*").execute().data
        session["ledger_df"] = pd.DataFrame(ledger_rows)

        bot("Fetching risk data from Supabase…")
        risk_rows = supabase.table("risk_table").select("*").execute().data
        session["risk_df"] = pd.DataFrame(risk_rows)

        show_summary(session["ledger_df"], "Ledger Balance")
        show_df(session["ledger_df"], "Ledger Balance (preview)")

        show_summary(session["risk_df"], "Risk Table")
        show_df(session["risk_df"], "Risk Table (preview)")

        return True
    except Exception as e:
        bot(f"[red]Error fetching data: {e}[/]")
        return False

# ── Reconciliation Engine ─────────────────────────────────────────────────────
def perform_recon(ledger_df: pd.DataFrame, risk_df: pd.DataFrame,
                  key_attrs: list, agg_attrs: list) -> pd.DataFrame:
    # Aggregate each side
    ledger_agg = ledger_df.groupby(key_attrs)[agg_attrs].sum().reset_index()
    risk_agg   = risk_df.groupby(key_attrs)[agg_attrs].sum().reset_index()

    ledger_agg = ledger_agg.rename(columns={c: f"ledger_{c}" for c in agg_attrs})
    risk_agg   = risk_agg.rename(columns={c: f"risk_{c}"   for c in agg_attrs})

    merged = pd.merge(ledger_agg, risk_agg, on=key_attrs, how="outer", indicator=True)

    # Compute differences and classify
    merged["recon_status"] = "MATCHED"
    merged.loc[merged["_merge"] == "left_only",  "recon_status"] = "LEDGER_ONLY"
    merged.loc[merged["_merge"] == "right_only", "recon_status"] = "RISK_ONLY"

    for col in agg_attrs:
        merged[f"{col}_diff"] = (
            merged[f"ledger_{col}"].fillna(0) - merged[f"risk_{col}"].fillna(0)
        )
        mask_break = (
            (merged["recon_status"] == "MATCHED") &
            (merged[f"{col}_diff"].abs() > 0.01)
        )
        merged.loc[mask_break, "recon_status"] = "AMOUNT_BREAK"

    merged = merged.drop(columns=["_merge"])
    return merged

def display_recon(recon_df: pd.DataFrame, agg_attrs: list):
    status_counts = recon_df["recon_status"].value_counts()

    # Summary panel
    lines = ["[bold]Reconciliation Results[/]\n"]
    status_colors = {
        "MATCHED"     : "green",
        "AMOUNT_BREAK": "yellow",
        "LEDGER_ONLY" : "red",
        "RISK_ONLY"   : "magenta",
    }
    for status, color in status_colors.items():
        count = status_counts.get(status, 0)
        lines.append(f"  [{color}]{status:<14}[/] : {count:>4}")

    total_diff = sum(
        recon_df[f"{col}_diff"].abs().sum()
        for col in agg_attrs if f"{col}_diff" in recon_df.columns
    )
    lines.append(f"\n  Total Amount Discrepancy: [bold red]{total_diff:,.2f}[/]")
    console.print(Panel("\n".join(lines), title="[bold blue]Recon Summary[/]", border_style="blue"))

    # Breaks detail
    breaks = recon_df[recon_df["recon_status"] != "MATCHED"]
    if not breaks.empty:
        show_df(breaks, "Breaks & Discrepancies", max_rows=20)
    else:
        bot("[green]No breaks found — datasets are fully reconciled.[/]")

def export_recon(recon_df: pd.DataFrame):
    path = "recon_output.csv"
    recon_df.to_csv(path, index=False)
    bot(f"Recon results exported to [bold]{path}[/]")

# ── Conversation Loop ─────────────────────────────────────────────────────────
def add_to_history(role: str, content: str):
    session["history"].append({"role": role, "content": content})

def handle_initial(user_msg: str) -> bool:
    """Returns True to continue, False to exit."""
    intent = detect_intent(user_msg)
    add_to_history("user", user_msg)

    if intent == "QUIT":
        bot("Goodbye. Stay compliant!")
        return False

    if intent == "FETCH_DATA":
        ok = fetch_data()
        if ok:
            session["phase"] = "fetched"
            reply = "Data loaded successfully. Should I run a reconciliation between the ledger and risk datasets?"
            bot(reply)
            add_to_history("assistant", reply)
            session["phase"] = "recon_confirm"
        return True

    # Generic LLM response for anything else
    reply = llm(session["history"] + [{"role": "user", "content": user_msg}])
    bot(reply)
    add_to_history("assistant", reply)
    return True

def handle_recon_confirm(user_msg: str) -> bool:
    intent = detect_intent(user_msg)
    add_to_history("user", user_msg)

    if intent == "QUIT":
        bot("Goodbye. Stay compliant!")
        return False

    if intent == "RECON_YES":
        available = COMMON_COLUMNS
        reply = (
            f"Great! The common columns available for reconciliation are:\n"
            f"  [bold]{available}[/]\n\n"
            "Please tell me the [bold]key attributes[/] to group/join on "
            "(e.g. 'account_id and currency')."
        )
        bot(reply)
        add_to_history("assistant", reply)
        session["phase"] = "key_attrs"
        return True

    if intent == "RECON_NO":
        reply = "No problem. You can ask me anything about the data, or say 'quit' to exit."
        bot(reply)
        add_to_history("assistant", reply)
        session["phase"] = "fetched"
        return True

    reply = llm(session["history"] + [{"role": "user", "content": user_msg}])
    bot(reply)
    add_to_history("assistant", reply)
    return True

def handle_key_attrs(user_msg: str) -> bool:
    add_to_history("user", user_msg)
    intent = detect_intent(user_msg)
    if intent == "QUIT":
        bot("Goodbye!")
        return False

    attrs = extract_attributes(user_msg, COMMON_COLUMNS, "key/join attributes")
    if not attrs:
        bot(f"I couldn't identify valid key attributes. Available: {COMMON_COLUMNS}. Please try again.")
        return True

    session["key_attrs"] = attrs
    reply = (
        f"Key attributes set: [bold yellow]{attrs}[/]\n\n"
        "Now tell me the [bold]aggregate attributes[/] to reconcile "
        "(e.g. 'amount')."
    )
    bot(reply)
    add_to_history("assistant", reply)
    session["phase"] = "agg_attrs"
    return True

def handle_agg_attrs(user_msg: str) -> bool:
    add_to_history("user", user_msg)
    intent = detect_intent(user_msg)
    if intent == "QUIT":
        bot("Goodbye!")
        return False

    numeric_cols = ["amount"]  # columns that exist in both tables and are numeric
    attrs = extract_attributes(user_msg, numeric_cols, "aggregate/sum attributes")
    if not attrs:
        bot(f"I couldn't identify valid aggregate attributes. Available: {numeric_cols}. Try again.")
        return True

    session["agg_attrs"] = attrs
    bot(f"Aggregate attributes set: [bold yellow]{attrs}[/]\n\nRunning reconciliation…")

    recon_df = perform_recon(
        session["ledger_df"], session["risk_df"],
        session["key_attrs"], session["agg_attrs"]
    )
    session["recon_df"] = recon_df

    display_recon(recon_df, session["agg_attrs"])

    bot("Generating regulatory insights…")
    insights = ai_insights(recon_df, session["key_attrs"], session["agg_attrs"])
    console.print(Panel(insights, title="[bold red]Regulatory Insights[/]", border_style="red"))

    add_to_history("assistant", f"Recon complete. Insights: {insights}")
    session["phase"] = "done"

    reply = (
        "What would you like to do next?\n"
        "  • [bold]Show breaks[/]   — view only the discrepant records\n"
        "  • [bold]Export[/]        — save results to CSV\n"
        "  • [bold]New recon[/]     — run another reconciliation\n"
        "  • [bold]Quit[/]          — exit"
    )
    bot(reply)
    return True

def handle_done(user_msg: str) -> bool:
    intent = detect_intent(user_msg)
    add_to_history("user", user_msg)

    if intent == "QUIT":
        bot("Goodbye! Stay compliant.")
        return False

    if intent == "EXPORT":
        export_recon(session["recon_df"])
        return True

    if intent == "SHOW_BREAKS":
        breaks = session["recon_df"][session["recon_df"]["recon_status"] != "MATCHED"]
        if breaks.empty:
            bot("No breaks found — all records are matched.")
        else:
            show_df(breaks, "Breaks Only", max_rows=50)
        return True

    if intent == "SHOW_SUMMARY":
        display_recon(session["recon_df"], session["agg_attrs"])
        return True

    # Allow re-running recon
    if "new recon" in user_msg.lower() or "another recon" in user_msg.lower():
        session["phase"] = "recon_confirm"
        reply = "Sure! Should I run another reconciliation with different attributes?"
        bot(reply)
        add_to_history("assistant", reply)
        return True

    # Fallback: let OpenAI answer contextually
    context_msg = (
        f"The user has completed a reconciliation. Recon status counts: "
        f"{session['recon_df']['recon_status'].value_counts().to_dict()}. "
        f"User asks: {user_msg}"
    )
    reply = llm(session["history"] + [{"role": "user", "content": context_msg}])
    bot(reply)
    add_to_history("assistant", reply)
    return True

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    console.print(Panel(
        "[bold cyan]RegBot[/] — Smart Regulatory Reporting Analyst\n"
        "Powered by [bold]OpenAI[/] + [bold]Supabase[/]\n"
        "[dim]Type 'quit' at any time to exit.[/]",
        border_style="cyan",
    ))

    bot("What do you want to do today?")

    handlers = {
        "initial"      : handle_initial,
        "fetched"      : handle_initial,      # allow re-fetch or other queries
        "recon_confirm": handle_recon_confirm,
        "key_attrs"    : handle_key_attrs,
        "agg_attrs"    : handle_agg_attrs,
        "done"         : handle_done,
    }

    while True:
        try:
            user_msg = user_prompt()
            if not user_msg:
                continue
            phase = session["phase"]
            handler = handlers.get(phase, handle_initial)
            should_continue = handler(user_msg)
            if not should_continue:
                break
        except KeyboardInterrupt:
            bot("\nInterrupted. Goodbye!")
            break
        except Exception as e:
            bot(f"[red]Unexpected error: {e}[/]")

if __name__ == "__main__":
    main()
