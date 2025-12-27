#!/bin/bash
#
# iTop to Elasticsearch - SQL File Approach (CORREGIDO)
#

set -e

EXPORT_DIR="/var/log/itop-exports"
DATE_TAG=$(date +%Y.%m.%d)
DB_HOST="yaruma.om.do"
DB_NAME="test_db"
DB_USER="itop"
DB_PASS="admin"
MINUTES_BACK=${MINUTES_BACK:-129600}

mkdir -p "$EXPORT_DIR"/{userrequest,incident}
find "$EXPORT_DIR" -name "*.ndjson" -mtime +5 -delete 2>/dev/null || true

echo "=========================================="
echo "iTop Export - $(date)"
echo "=========================================="

# ============================================================================
# USERREQUESTS
# ============================================================================
echo "Exporting UserRequests..."

cat > /tmp/export_userrequest.sql <<'EOSQL'
SELECT CONCAT(
  '{"@timestamp":"', DATE_FORMAT(NOW(), '%Y-%m-%dT%H:%i:%s.000Z'), '",',
  '"ticket_type":"UserRequest",',
  '"workorder":{',
    '"id":', COALESCE(wo.id, 'null'), ',',
    '"name":"', COALESCE(REPLACE(REPLACE(wo.name, '"', '\\"'), CHAR(10), ' '), ''), '",',
    '"status":"', COALESCE(wo.status, ''), '",',
    '"start_date":"', COALESCE(DATE_FORMAT(wo.start_date, '%Y-%m-%dT%H:%i:%s.000Z'), ''), '",',
    '"end_date":"', COALESCE(DATE_FORMAT(wo.end_date, '%Y-%m-%dT%H:%i:%s.000Z'), ''), '",',
    '"ref":"', COALESCE(wo.ref, ''), '"',
  '},',
  '"ticket":{',
    '"id":', t.id, ',',
    '"ref":"', COALESCE(t.ref, ''), '",',
    '"title":"', COALESCE(REPLACE(REPLACE(t.title, '"', '\\"'), CHAR(10), ' '), ''), '",',
    '"operational_status":"', COALESCE(t.operational_status, ''), '",',
    '"status":"', COALESCE(tr.status, ''), '",',
    '"priority":"', COALESCE(tr.priority, ''), '",',
    '"start_date":"', COALESCE(DATE_FORMAT(t.start_date, '%Y-%m-%dT%H:%i:%s.000Z'), ''), '",',
    '"last_update":"', COALESCE(DATE_FORMAT(t.last_update, '%Y-%m-%dT%H:%i:%s.000Z'), ''), '",',
    '"tto_timespent":', COALESCE(tr.tto_timespent, 0), ',',
    '"ttr_timespent":', COALESCE(tr.ttr_timespent, 0), ',',
    '"resolution_code":"', COALESCE(tr.resolution_code, ''), '",',
    '"user_satisfaction":"', COALESCE(tr.user_satisfaction, ''), '"',
  '},',
  '"organization":{',
    '"id":', COALESCE(org.id, 0), ',',
    '"name":"', COALESCE(org.name, '') , '"',
  '},',
  '"caller":{',
    '"id":', COALESCE(t.caller_id, 0), ',',
    '"name":"', COALESCE(CONCAT(p_caller.first_name, ' ', c_caller.name), c_caller.name, ''), '"',
  '},',
  '"agent":{',
    '"id":', COALESCE(t.agent_id, 0), ',',
    '"name":"', COALESCE(CONCAT(p_agent.first_name, ' ', c_agent.name), c_agent.name, ''), '"',
  '},',
  '"team":{',
    '"id":', COALESCE(team.id, 0), ',',
    '"name":"', COALESCE(team.name, ''), '"',
  '},',
  '"service":{',
    '"id":', COALESCE(service.id, 0), ',',
    '"name":"', COALESCE(service.name, ''), '"',
  '}',
  '}'
)
FROM ticket t
INNER JOIN ticket_request tr ON t.id = tr.id
LEFT JOIN workorder wo ON wo.ticket_id = t.id
LEFT JOIN organization org ON t.org_id = org.id
LEFT JOIN contact c_caller ON t.caller_id = c_caller.id
LEFT JOIN person p_caller ON t.caller_id = p_caller.id
LEFT JOIN contact c_agent ON t.agent_id = c_agent.id
LEFT JOIN person p_agent ON t.agent_id = p_agent.id
LEFT JOIN contact team ON t.team_id = team.id
LEFT JOIN service service ON tr.service_id = service.id
WHERE t.finalclass = 'UserRequest'
  AND t.last_update >= DATE_SUB(NOW(), INTERVAL MINUTES_BACK_PLACEHOLDER MINUTE);
EOSQL

sed -i "s/MINUTES_BACK_PLACEHOLDER/${MINUTES_BACK}/g" /tmp/export_userrequest.sql
OUTPUT_FILE="$EXPORT_DIR/userrequest/itop-userrequest-${DATE_TAG}.ndjson"
mysql -h${DB_HOST} -u${DB_USER} -p${DB_PASS} ${DB_NAME} -N -r < /tmp/export_userrequest.sql > "$OUTPUT_FILE"

echo "  ✓ Exported $(wc -l < "$OUTPUT_FILE") UserRequests"

# ============================================================================
# INCIDENTS
# ============================================================================
echo "Exporting Incidents..."

cat > /tmp/export_incident.sql <<'EOSQL'
SELECT CONCAT(
  '{"@timestamp":"', DATE_FORMAT(NOW(), '%Y-%m-%dT%H:%i:%s.000Z'), '",',
  '"ticket_type":"Incident",',
  '"workorder":{',
    '"id":', COALESCE(wo.id, 'null'), ',',
    '"name":"', COALESCE(REPLACE(REPLACE(wo.name, '"', '\\"'), CHAR(10), ' '), ''), '",',
    '"status":"', COALESCE(wo.status, ''), '"',
  '},',
  '"ticket":{',
    '"id":', t.id, ',',
    '"ref":"', COALESCE(t.ref, ''), '",',
    '"title":"', COALESCE(REPLACE(REPLACE(t.title, '"', '\\"'), CHAR(10), ' '), ''), '",',
    '"status":"', COALESCE(ti.status, ''), '",',
    '"priority":"', COALESCE(ti.priority, ''), '",',
    '"last_update":"', COALESCE(DATE_FORMAT(t.last_update, '%Y-%m-%dT%H:%i:%s.000Z'), ''), '"',
  '},',
  '"caller":{',
    '"id":', COALESCE(t.caller_id, 0), ',',
    '"name":"', COALESCE(CONCAT(p_caller.first_name, ' ', c_caller.name), c_caller.name, ''), '"',
  '},',
  '"agent":{',
    '"id":', COALESCE(t.agent_id, 0), ',',
    '"name":"', COALESCE(CONCAT(p_agent.first_name, ' ', c_agent.name), c_agent.name, ''), '"',
  '}'
  '}'
)
FROM ticket t
INNER JOIN ticket_incident ti ON t.id = ti.id
LEFT JOIN workorder wo ON wo.ticket_id = t.id
LEFT JOIN contact c_caller ON t.caller_id = c_caller.id
LEFT JOIN person p_caller ON t.caller_id = p_caller.id
LEFT JOIN contact c_agent ON t.agent_id = c_agent.id
LEFT JOIN person p_agent ON t.agent_id = p_agent.id
WHERE t.finalclass = 'Incident'
  AND t.last_update >= DATE_SUB(NOW(), INTERVAL MINUTES_BACK_PLACEHOLDER MINUTE);
EOSQL

sed -i "s/MINUTES_BACK_PLACEHOLDER/${MINUTES_BACK}/g" /tmp/export_incident.sql
OUTPUT_FILE="$EXPORT_DIR/incident/itop-incident-${DATE_TAG}.ndjson"
mysql -h${DB_HOST} -u${DB_USER} -p${DB_PASS} ${DB_NAME} -N -r < /tmp/export_incident.sql > "$OUTPUT_FILE"

echo "  ✓ Exported $(wc -l < "$OUTPUT_FILE") Incidents"

# Limpieza
rm -f /tmp/export_userrequest.sql /tmp/export_incident.sql

echo "=========================================="
echo "Completed: $(date)"
echo "=========================================="
