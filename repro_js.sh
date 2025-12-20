#!/bin/bash
set -e

MASTER_URL="http://127.0.0.1:8080"

echo "Submitting JS Job..."

# Create a JS file with runtime error
cat <<EOF > /tmp/js_re.json
{
  "language": "javascript",
  "source_code": "console.log(undefinedVariable.property);",
  "test_cases": [
    {
      "id": "0",
      "input": "",
      "expected_output": "ignore"
    }
  ]
}
EOF

# Submit
RESPONSE=$(curl -s -X POST $MASTER_URL/submit -H "Content-Type: application/json" -d @/tmp/js_re.json)
echo "Response: $RESPONSE"
JOB_ID=$(echo $RESPONSE | jq -r .job_id)

echo "Job ID: $JOB_ID"

# Monitor
python3 scripts/monitor.py $MASTER_URL $JOB_ID
