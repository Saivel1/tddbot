#!/bin/bash

# tests/locust/run_load_test.sh

SCENARIO=${1:-load}  # По умолчанию "load"
HOST=${2:-https://your-domain.com}

echo "🚀 Starting Locust with scenario: $SCENARIO"
echo "🎯 Target: $HOST"
echo ""

case $SCENARIO in
    smoke)
        locust -f locustfile.py \
            --host=$HOST \
            --users=10 \
            --spawn-rate=2 \
            --run-time=2m \
            --html=../results/smoke_report.html \
            --csv=../results/smoke
        ;;
    load)
        locust -f locustfile.py \
            --host=$HOST \
            --users=100 \
            --spawn-rate=10 \
            --run-time=5m \
            --html=../results/load_report.html \
            --csv=../results/load
        ;;
    stress)
        locust -f locustfile.py \
            --host=$HOST \
            --users=500 \
            --spawn-rate=50 \
            --run-time=10m \
            --html=../results/stress_report.html \
            --csv=../results/stress
        ;;
    spike)
        locust -f locustfile.py \
            --host=$HOST \
            --users=1000 \
            --spawn-rate=1000 \
            --run-time=2m \
            --html=../results/spike_report.html \
            --csv=../results/spike
        ;;
    soak)
        locust -f locustfile.py \
            --host=$HOST \
            --users=200 \
            --spawn-rate=20 \
            --run-time=60m \
            --html=../results/soak_report.html \
            --csv=../results/soak
        ;;
    web)
        echo "🌐 Starting Locust Web UI"
        locust -f locustfile.py --host=$HOST
        ;;
    *)
        echo "❌ Unknown scenario: $SCENARIO"
        echo "Available: smoke, load, stress, spike, soak, web"
        exit 1
        ;;
esac

echo ""
echo "✅ Test completed! Check results in tests/results/"