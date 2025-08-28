#!/usr/bin/env python3
from flask import Flask, request, jsonify, render_template
import json
import datetime
from pprint import pprint
from collections import deque

app = Flask(__name__)

alert_history = deque(maxlen=100)
alert_stats = {
    'total_received': 0,
    'critical_alerts': 0,
    'warning_alerts': 0,
    'last_alert_time': None
}

@app.route('/webhook', methods=['POST'])
def receive_alert():
    try:
        alert_data = request.json
        timestamp = datetime.datetime.now()
        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        
        print("=" * 80)
        print(f"SEISMIC ALERT RECEIVED at {timestamp_str}")
        print("=" * 80)
        
        if alert_data and 'alerts' in alert_data:
            print(f"Number of alerts: {len(alert_data['alerts'])}")
            print(f"Group Labels: {alert_data.get('groupLabels', {})}")
            print(f"Common Labels: {alert_data.get('commonLabels', {})}")
            print()
            
            for i, alert in enumerate(alert_data['alerts'], 1):
                alert_entry = {
                    'timestamp': timestamp,
                    'alertname': alert.get('labels', {}).get('alertname', 'Unknown'),
                    'status': alert.get('status', 'Unknown'),
                    'severity': alert.get('labels', {}).get('severity', 'Unknown'),
                    'region': alert.get('labels', {}).get('region', 'N/A'),
                    'summary': alert.get('annotations', {}).get('summary', 'N/A'),
                    'description': alert.get('annotations', {}).get('description', 'N/A'),
                    'startsAt': alert.get('startsAt', 'N/A'),
                    'endsAt': alert.get('endsAt', 'N/A'),
                    'raw_alert': alert
                }
                
                alert_history.appendleft(alert_entry)
                
                alert_stats['total_received'] += 1
                alert_stats['last_alert_time'] = timestamp
                if alert_entry['severity'] == 'critical':
                    alert_stats['critical_alerts'] += 1
                elif alert_entry['severity'] == 'warning':
                    alert_stats['warning_alerts'] += 1
                
                print(f"Alert #{i}:")
                print(f"  Name: {alert_entry['alertname']}")
                print(f"  Status: {alert_entry['status']}")
                print(f"  Severity: {alert_entry['severity']}")
                print(f"  Region: {alert_entry['region']}")
                print(f"  Summary: {alert_entry['summary']}")
                print(f"  Description: {alert_entry['description']}")
                
                if alert_entry['status'] == 'firing':
                    print(f"  Started At: {alert_entry['startsAt']}")
                elif alert_entry['status'] == 'resolved':
                    print(f"  Resolved At: {alert_entry['endsAt']}")
                
                print()
        
        print("Raw JSON payload:")
        print(json.dumps(alert_data, indent=2, default=str))
        print("=" * 80)
        
        return jsonify({"status": "received", "timestamp": timestamp_str}), 200
        
    except Exception as e:
        print(f"Error processing alert: {e}")
        return jsonify({"error": str(e)}), 400

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy", 
        "service": "seismic-webhook-receiver",
        "timestamp": datetime.datetime.now().isoformat()
    }), 200

@app.route('/', methods=['POST'])
def receive_alert_legacy():
    return receive_alert()

@app.route('/', methods=['GET'])
def dashboard():
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    alerts = []
    for alert in list(alert_history)[:20]:
        alerts.append({
            'alertname': alert['alertname'],
            'status': alert['status'],
            'severity': alert['severity'],
            'region': alert['region'],
            'summary': alert['summary'],
            'description': alert['description'],
            'startsAt': alert['startsAt'],
            'timestamp_str': alert['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        })
    
    last_alert_time = "Never" if not alert_stats['last_alert_time'] else alert_stats['last_alert_time'].strftime('%H:%M:%S')
    
    return render_template('dashboard.html',
        current_time=current_time,
        total_alerts=alert_stats['total_received'],
        critical_alerts=alert_stats['critical_alerts'],
        warning_alerts=alert_stats['warning_alerts'],
        last_alert=last_alert_time,
        alerts=alerts
    )

@app.route('/api/info', methods=['GET'])
def api_info():
    return jsonify({
        "service": "Seismic Alert Webhook Receiver",
        "endpoints": {
            "POST /": "Receive AlertManager webhooks (legacy)",
            "POST /webhook": "Receive AlertManager webhooks",
            "GET /": "HTML Dashboard",
            "GET /health": "Health check",
            "GET /api/info": "This API info"
        },
        "listening": "0.0.0.0:5001",
        "stats": alert_stats
    }), 200

if __name__ == '__main__':
    print("Starting Seismic Alert Webhook Receiver...")
    print("=" * 60)
    
    app.run(host='0.0.0.0', port=5001, debug=True)