"""
Real-time Performance Dashboard for Trading Bot
Run with: python performance_dashboard.py
Access at: http://localhost:8050

PHASE 2: When TradingCore is injected via inject_core(), reads trade data
directly from core.state (SystemState) — no DB required, always authoritative.
Falls back to DatabaseService if running standalone.
"""

import dash
from dash import dcc, html, Input, Output
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
from services.database_service import DatabaseService

# ── TradingCore injection point ────────────────────────────────────────────────
_CORE = None   # set by inject_core() when called from bot.py

def inject_core(core) -> None:
    """
    Called by bot.py after TradingCore is created.
    After injection, get_trade_data() reads from core.state instead of DB.
    """
    global _CORE
    _CORE = core

# Initialize database connection (used only when _CORE is None)
db = DatabaseService()

# Create Dash app
app = dash.Dash(__name__, title='Trading Bot Dashboard')

# Print startup message ONCE (not in callback)
print("🚀 Starting Performance Dashboard...")
print("📊 Access at: http://localhost:8050")

# Define layout
app.layout = html.Div([
    html.H1("🚀 Trading Bot Performance Dashboard", 
            style={'textAlign': 'center', 'color': '#2c3e50', 'padding': '20px'}),
    
    # Refresh button and interval
    html.Div([
        html.Button('🔄 Refresh Data', id='refresh-button', n_clicks=0,
                   style={'backgroundColor': '#3498db', 'color': 'white', 
                          'padding': '10px 20px', 'border': 'none', 
                          'borderRadius': '5px', 'cursor': 'pointer'}),
        dcc.Interval(id='interval-component', interval=30*1000, n_intervals=0)  # 30 seconds
    ], style={'textAlign': 'center', 'margin': '20px'}),
    
    # Summary cards
    html.Div(id='summary-cards', style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'center'}),
    
    # Charts
    html.Div([
        # First row: Equity curve and Win rate by strategy
        html.Div([
            html.Div([
                html.H3("💰 Equity Curve", style={'textAlign': 'center'}),
                dcc.Graph(id='equity-chart')
            ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
            
            html.Div([
                html.H3("📊 Win Rate by Strategy", style={'textAlign': 'center'}),
                dcc.Graph(id='strategy-chart')
            ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
        ]),
        
        # Second row: Trades by hour and Asset performance
        html.Div([
            html.Div([
                html.H3("⏰ Best Trading Hours", style={'textAlign': 'center'}),
                dcc.Graph(id='hourly-chart')
            ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
            
            html.Div([
                html.H3("📈 Asset Performance", style={'textAlign': 'center'}),
                dcc.Graph(id='asset-chart')
            ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
        ]),
    ]),
    
    # Recent trades table
    html.Div([
        html.H3("📋 Recent Trades", style={'textAlign': 'center'}),
        html.Div(id='recent-trades')
    ], style={'padding': '20px'}),
    
], style={'backgroundColor': '#f5f6fa', 'padding': '20px'})

def get_trade_data():
    """
    Fetch trade data.
    PHASE 2: reads from TradingCore.state when injected (no DB required).
    Falls back to DatabaseService query for standalone mode.
    """
    # ── Primary: TradingCore.state ────────────────────────────────────────────
    if _CORE is not None:
        try:
            closed = _CORE.state.get_closed_positions(limit=500)
            if not closed:
                return pd.DataFrame()
            df = pd.DataFrame(closed)
            # Normalise column names to match DB schema expected by charts below
            if 'entry_time' in df.columns:
                df['entry_time'] = pd.to_datetime(df['entry_time'], errors='coerce')
            if 'exit_time' in df.columns:
                df['exit_time']  = pd.to_datetime(df['exit_time'],  errors='coerce')
            for col in ['pnl', 'pnl_percent', 'confidence']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            # Ensure required columns exist
            for col in ['strategy_id', 'asset', 'direction', 'exit_reason']:
                if col not in df.columns:
                    df[col] = 'unknown'
            return df
        except Exception as e:
            print(f"TradingCore state read error: {e}")

    # ── Fallback: DatabaseService ─────────────────────────────────────────────
    try:
        query = """
        SELECT
            trade_id, asset, direction, entry_price, exit_price,
            pnl, pnl_percent, entry_time, exit_time, exit_reason,
            strategy_id, confidence
        FROM trades
        WHERE exit_time IS NOT NULL
        ORDER BY entry_time DESC
        """
        df = pd.read_sql(query, db.session.bind)
        df['entry_time'] = pd.to_datetime(df['entry_time'])
        df['exit_time']  = pd.to_datetime(df['exit_time'])
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

def calculate_equity_curve(df):
    """Calculate cumulative P&L over time"""
    if df.empty:
        return pd.DataFrame()
    
    df = df.sort_values('exit_time')
    df['cumulative_pnl'] = df['pnl'].cumsum()
    return df

@app.callback(
    [Output('summary-cards', 'children'),
     Output('equity-chart', 'figure'),
     Output('strategy-chart', 'figure'),
     Output('hourly-chart', 'figure'),
     Output('asset-chart', 'figure'),
     Output('recent-trades', 'children')],
    [Input('refresh-button', 'n_clicks'),
     Input('interval-component', 'n_intervals')]
)
def update_dashboard(n_clicks, n_intervals):
    """Update all dashboard components"""
    
    df = get_trade_data()
    
    # ===== SUMMARY CARDS =====
    card_style = {
        'backgroundColor': 'white',
        'borderRadius': '10px',
        'padding': '20px',
        'margin': '10px',
        'boxShadow': '0 2px 5px rgba(0,0,0,0.1)',
        'minWidth': '150px',
        'textAlign': 'center'
    }
    
    if not df.empty:
        total_trades = len(df)
        winning_trades = len(df[df['pnl'] > 0])
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        total_pnl = df['pnl'].sum()
        avg_pnl = df['pnl'].mean()
        
        # Today's trades
        today = datetime.now().date()
        today_trades = df[df['exit_time'].dt.date == today]
        today_pnl = today_trades['pnl'].sum() if not today_trades.empty else 0
    else:
        total_trades = winning_trades = win_rate = total_pnl = avg_pnl = today_pnl = 0
    
    cards = html.Div([
        html.Div([
            html.H4("Total Trades"),
            html.H2(f"{total_trades}", style={'color': '#2980b9'})
        ], style=card_style),
        
        html.Div([
            html.H4("Win Rate"),
            html.H2(f"{win_rate:.1f}%", style={'color': '#27ae60'})
        ], style=card_style),
        
        html.Div([
            html.H4("Total P&L"),
            html.H2(f"${total_pnl:.2f}", style={'color': '#e67e22' if total_pnl >= 0 else '#e74c3c'})
        ], style=card_style),
        
        html.Div([
            html.H4("Today's P&L"),
            html.H2(f"${today_pnl:.2f}", style={'color': '#27ae60' if today_pnl >= 0 else '#e74c3c'})
        ], style=card_style),
        
        html.Div([
            html.H4("Avg Trade"),
            html.H2(f"${avg_pnl:.2f}", style={'color': '#8e44ad'})
        ], style=card_style),
    ], style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'center'})
    
    # ===== EQUITY CURVE =====
    if not df.empty:
        equity_df = calculate_equity_curve(df)
        equity_fig = px.line(equity_df, x='exit_time', y='cumulative_pnl',
                            title='Equity Curve (Cumulative P&L)')
        equity_fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Cumulative P&L ($)",
            hovermode='x unified'
        )
    else:
        equity_fig = go.Figure()
        equity_fig.add_annotation(text="No data yet", showarrow=False)
    
    # ===== STRATEGY PERFORMANCE =====
    if not df.empty:
        strategy_stats = df.groupby('strategy_id').agg({
            'pnl': ['count', 'sum', 'mean']
        }).round(2)
        
        strategy_stats.columns = ['trades', 'total_pnl', 'avg_pnl']
        strategy_stats = strategy_stats.reset_index()
        
        # Calculate win rate per strategy
        win_rates = []
        for strategy in strategy_stats['strategy_id']:
            strategy_trades = df[df['strategy_id'] == strategy]
            wins = len(strategy_trades[strategy_trades['pnl'] > 0])
            total = len(strategy_trades)
            win_rates.append(wins / total * 100 if total > 0 else 0)
        
        strategy_stats['win_rate'] = win_rates
        
        strategy_fig = px.bar(strategy_stats, x='strategy_id', y='win_rate',
                             title='Win Rate by Strategy',
                             color='win_rate',
                             color_continuous_scale='RdYlGn')
        strategy_fig.update_layout(xaxis_title="Strategy", yaxis_title="Win Rate (%)")
    else:
        strategy_fig = go.Figure()
        strategy_fig.add_annotation(text="No data yet", showarrow=False)
    
    # ===== TRADES BY HOUR =====
    if not df.empty:
        df['hour'] = df['entry_time'].dt.hour
        hourly_stats = df.groupby('hour').agg({
            'pnl': ['count', 'sum', 'mean']
        }).round(2)
        hourly_stats.columns = ['trades', 'total_pnl', 'avg_pnl']
        hourly_stats = hourly_stats.reset_index()
        
        hourly_fig = px.bar(hourly_stats, x='hour', y='total_pnl',
                           title='P&L by Trading Hour',
                           color='total_pnl',
                           color_continuous_scale='RdYlGn')
        hourly_fig.update_layout(xaxis_title="Hour of Day (EAT)", yaxis_title="Total P&L ($)")
    else:
        hourly_fig = go.Figure()
        hourly_fig.add_annotation(text="No data yet", showarrow=False)
    
    # ===== ASSET PERFORMANCE =====
    if not df.empty:
        asset_stats = df.groupby('asset').agg({
            'pnl': ['count', 'sum', 'mean']
        }).round(2)
        asset_stats.columns = ['trades', 'total_pnl', 'avg_pnl']
        asset_stats = asset_stats.reset_index()
        asset_stats = asset_stats.sort_values('total_pnl', ascending=False).head(10)
        
        asset_fig = px.bar(asset_stats, x='asset', y='total_pnl',
                          title='Top 10 Assets by P&L',
                          color='total_pnl',
                          color_continuous_scale='RdYlGn')
        asset_fig.update_layout(xaxis_title="Asset", yaxis_title="Total P&L ($)")
    else:
        asset_fig = go.Figure()
        asset_fig.add_annotation(text="No data yet", showarrow=False)
    
    # ===== RECENT TRADES TABLE =====
    if not df.empty:
        recent = df.head(10)[['entry_time', 'asset', 'direction', 'pnl', 'pnl_percent', 
                              'exit_reason', 'strategy_id']]
        recent['entry_time'] = recent['entry_time'].dt.strftime('%Y-%m-%d %H:%M')
        recent['pnl'] = recent['pnl'].round(2)
        recent['pnl_percent'] = recent['pnl_percent'].round(2)
        
        # Create table rows
        table_rows = []
        for i in range(len(recent)):
            row_color = '#f0fff0' if recent.iloc[i]['pnl'] > 0 else '#fff0f0'
            row = html.Tr([
                html.Td(recent.iloc[i]['entry_time']),
                html.Td(recent.iloc[i]['asset']),
                html.Td(recent.iloc[i]['direction']),
                html.Td(f"${recent.iloc[i]['pnl']:.2f}"),
                html.Td(f"{recent.iloc[i]['pnl_percent']:.2f}%"),
                html.Td(recent.iloc[i]['exit_reason']),
                html.Td(recent.iloc[i]['strategy_id']),
            ], style={'backgroundColor': row_color})
            table_rows.append(row)
        
        table = html.Table([
            html.Thead(html.Tr([
                html.Th("Time"), html.Th("Asset"), html.Th("Direction"), 
                html.Th("P&L"), html.Th("P&L %"), html.Th("Exit Reason"), html.Th("Strategy")
            ], style={'backgroundColor': '#34495e', 'color': 'white', 'padding': '10px'}))
        ] + table_rows, style={'width': '100%', 'borderCollapse': 'collapse'})
        
    else:
        table = html.Div("No trades yet")
    
    return cards, equity_fig, strategy_fig, hourly_fig, asset_fig, table

if __name__ == '__main__':
    # Print only once here
    app.run(debug=True, host='0.0.0.0', port=8050)