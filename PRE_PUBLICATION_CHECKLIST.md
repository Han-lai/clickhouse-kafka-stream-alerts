# Pre-Publication Security Checklist

Before publishing this repository to GitHub, manually review and address the following items:

## 🔴 Critical - Must Fix

### 1. Hard-coded Credentials
**File**: `kafka_clickhouse_alert_project/src/config.py`

```python
# Lines 17-19
CLICKHOUSE_USER = 'default'  # ⚠️ REPLACE WITH PLACEHOLDER
CLICKHOUSE_PASSWORD = 'default'  # ⚠️ REPLACE WITH PLACEHOLDER
```

**Action**: Replace with environment variable references or placeholder text:
```python
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'your_username')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'your_password')
```

### 2. Internal IP Addresses
**Files**: 
- `kafka_clickhouse_alert_project/src/config.py` (lines 5, 16)
- `kafka_clickhouse_alert_project/kafka_message.py` (line 63)
- `kafka_clickhouse_alert_project/generate_fake_data.py` (imports from config)

**Current Values**:
```python
KAFKA_BROKER = '10.136.218.207:9092'  # ⚠️ INTERNAL IP
CLICKHOUSE_HOST = '10.136.218.207'    # ⚠️ INTERNAL IP
```

**Action**: Replace with generic placeholders:
```python
KAFKA_BROKER = 'localhost:9092'  # or 'kafka-broker:9092'
CLICKHOUSE_HOST = 'localhost'    # or 'clickhouse-server'
```

### 3. Company/Organization-Specific Names
**File**: `kafka_clickhouse_alert_project/src/config.py` (line 7)

```python
KAFKA_SOURCE_TOPIC = "MES-IT__DGPUBMES__DSM_POW__R_EQUIP_STATUS_T"
# ⚠️ Contains organizational identifiers: DGPUBMES, DSM_POW
```

**Action**: Replace with generic name:
```python
KAFKA_SOURCE_TOPIC = "device_status_events"
```

**File**: `kafka_clickhouse_alert_project/src/config.py` (line 20)

```python
CLICKHOUSE_DATABASE = 'tw_ait'  # ⚠️ May reveal organizational info
```

**Action**: Replace with generic name:
```python
CLICKHOUSE_DATABASE = 'device_monitoring'
```

## 🟡 Medium Priority - Review Recommended

### 4. Commented Code with Internal References
**File**: `kafka_clickhouse_alert_project/kafka_message.py` (lines 67-73)

```python
# from kafka import KafkaAdminClient
# admin_client = KafkaAdminClient(
#     bootstrap_servers="10.146.192.81:9092,10.146.192.82:9092,10.146.192.83:9092"
# )
# 印出所有 broker ID
# print("Cluster nodes:", admin_client.describe_cluster())
```

**Action**: Remove commented code or replace IPs with placeholders

**File**: `kafka_clickhouse_alert_project/generate_fake_data.py` (lines 88-95)

```python
# from kafka import KafkaAdminClient
# admin_client = KafkaAdminClient(
#     bootstrap_servers="10.146.192.81:9092,10.146.192.82:9092,10.146.192.83:9092"
# )
# 印出所有 broker ID
# print("Cluster nodes:", admin_client.describe_cluster())
```

**Action**: Remove commented code or replace IPs with placeholders

### 5. Log Files
**File**: `kafka_clickhouse_alert_project/setup_clickhouse.log`

**Action**: 
- Review log content for sensitive information
- Add to `.gitignore` if not already excluded
- Delete from repository if it contains sensitive data

### 6. Table and View Naming Conventions
**Files**: All SQL files in `kafka_clickhouse_alert_project/sql/`

**Current naming pattern**:
- `mes_source_kafka_engine` (MES = Manufacturing Execution System)
- `mes_source_mergetree`
- `mes_status_change_target_kafka_engine`
- `mes_temp_alert_target_kafka_engine`

**Action**: Consider renaming to generic names:
- `device_source_kafka_engine`
- `device_source_mergetree`
- `device_status_change_target_kafka_engine`
- `device_temp_alert_target_kafka_engine`

**Note**: This requires updating `config.py` and all SQL files

## 🟢 Low Priority - Optional Review

### 7. Virtual Environment Directory
**Directory**: `dfocvenv/`

**Action**: Ensure it's excluded in `.gitignore`:
```gitignore
dfocvenv/
venv/
*.pyc
__pycache__/
```

### 8. Python Cache Files
**Directory**: `kafka_clickhouse_alert_project/src/__pycache__/`

**Action**: Ensure excluded in `.gitignore`:
```gitignore
__pycache__/
*.py[cod]
*$py.class
```

### 9. Orphaned/Unused Files
**Files**:
- `kafka_clickhouse_alert_project/gen_fake_data.py` (duplicate?)
- `kafka_clickhouse_alert_project/setup_ch.py` (duplicate?)
- `setup_materialized_view.sql` (orphaned?)

**Action**: 
- Delete if truly unused
- Or add comment explaining their purpose

### 10. Missing Dependency Documentation
**Missing files**:
- `requirements.txt`
- `pyproject.toml`
- `setup.py`

**Action**: Create `requirements.txt`:
```txt
confluent-kafka>=2.0.0
clickhouse-driver>=0.2.0
```

### 11. Missing .gitignore
**Action**: Create `.gitignore`:
```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
venv/
ENV/
env/
dfocvenv/

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# Logs
*.log

# OS
.DS_Store
Thumbs.db
```

### 12. License File
**Missing file**: `LICENSE`

**Action**: Add appropriate license (MIT, Apache 2.0, etc.) or add disclaimer if no license

## Verification Steps

After making changes, verify:

1. **Search for IP patterns**:
   ```bash
   grep -r "10\.\d\+\.\d\+\.\d\+" .
   grep -r "192\.168\.\d\+\.\d\+" .
   ```

2. **Search for common credential keywords**:
   ```bash
   grep -ri "password" .
   grep -ri "secret" .
   grep -ri "token" .
   grep -ri "api_key" .
   ```

3. **Search for organization-specific terms**:
   ```bash
   grep -ri "dgpubmes" .
   grep -ri "dsm_pow" .
   grep -ri "tw_ait" .
   ```

4. **Check for sensitive file types**:
   ```bash
   find . -name "*.log"
   find . -name "*.key"
   find . -name "*.pem"
   find . -name ".env"
   ```

## Final Checklist

- [ ] All hard-coded credentials replaced with placeholders/env vars
- [ ] All internal IP addresses replaced with generic hostnames
- [ ] Company-specific names replaced with generic terms
- [ ] Commented code with sensitive info removed
- [ ] Log files reviewed and excluded
- [ ] `.gitignore` created and configured
- [ ] `requirements.txt` added
- [ ] `LICENSE` file added (or disclaimer added to README)
- [ ] Orphaned files removed or documented
- [ ] README.md reviewed for sensitive information
- [ ] All verification steps completed

## Recommended Sanitization Script

```bash
#!/bin/bash
# sanitize.sh - Run before git push

# Replace internal IPs
find . -type f -name "*.py" -exec sed -i 's/10\.136\.218\.207/localhost/g' {} +

# Replace organization-specific topic name
find . -type f -name "*.py" -exec sed -i 's/MES-IT__DGPUBMES__DSM_POW__R_EQUIP_STATUS_T/device_status_events/g' {} +

# Replace database name
find . -type f -name "*.py" -exec sed -i 's/tw_ait/device_monitoring/g' {} +

echo "Sanitization complete. Review changes before committing."
```

**Warning**: Test the script on a copy of the repository first!
