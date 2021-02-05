# ERP Dynamics 365 Integration

Integration project between SAP Business One and Dynamics 365 CRM, including API connectors, data synchronization, and analytics dashboards.

## Overview

This project demonstrates ERP integration capabilities:
- SAP B1 to Dynamics 365 CRM synchronization
- Python API connectors for both systems
- SQL staging tables for data transformation
- CRM synchronization logic and workflows
- ERP + CRM alignment dashboard

## Architecture

### Components
- **API Connectors**: Python modules for SAP B1 and Dynamics 365
- **Data Staging**: SQL Server tables for data transformation
- **Synchronization Engine**: Automated sync workflows
- **Dashboard**: Power BI dashboard for ERP + CRM metrics

## Integration Flow

1. **Extract**: Pull data from SAP Business One
2. **Transform**: Clean and normalize data in SQL staging
3. **Load**: Push synchronized data to Dynamics 365 CRM
4. **Monitor**: Track sync status and errors

## Requirements

- Python 3.8+
- Microsoft SQL Server
- SAP Business One API access
- Dynamics 365 CRM API access
- Power BI (for dashboard)

## License

MIT License















































