# Formula-One-Data

The following data set consists of the Drivers, Constructors, pit-stops, races and results data. 
We've cleaned the data and integreted the tables with the appropriate details.
We've saved the final DataFrames in /processed path in parquet format.
Post cleanup, we have associated the tables WRT the primary & foriegn keys for the tables
{
    "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
    "contentVersion": "1.0.0.0",
    "parameters": {
        "storageAccounts_formula1race02_name": {
            "defaultValue": "formula1race02",
            "type": "String"
        }
    },
    "variables": {},
    "resources": [
        {
            "type": "Microsoft.Storage/storageAccounts",
            "apiVersion": "2021-09-01",
            "name": "[parameters('storageAccounts_formula1race02_name')]",
            "location": "uksouth",
            "sku": {
                "name": "Standard_LRS",
                "tier": "Standard"
            },
            "kind": "StorageV2",
            "properties": {
                "dnsEndpointType": "Standard",
                "defaultToOAuthAuthentication": false,
                "publicNetworkAccess": "Enabled",
                "allowCrossTenantReplication": false,
                "isSftpEnabled": false,
                "minimumTlsVersion": "TLS1_2",
                "allowBlobPublicAccess": true,
                "allowSharedKeyAccess": true,
                "isHnsEnabled": true,
                "networkAcls": {
                    "bypass": "AzureServices",
                    "virtualNetworkRules": [],
                    "ipRules": [],
                    "defaultAction": "Allow"
                },
                "supportsHttpsTrafficOnly": true,
                "encryption": {
                    "requireInfrastructureEncryption": false,
                    "services": {
                        "file": {
                            "keyType": "Account",
                            "enabled": true
                        },
                        "blob": {
                            "keyType": "Account",
                            "enabled": true
                        }
                    },
                    "keySource": "Microsoft.Storage"
                },
                "accessTier": "Hot"
            }
        },
        {
            "type": "Microsoft.Storage/storageAccounts/blobServices",
            "apiVersion": "2021-09-01",
            "name": "[concat(parameters('storageAccounts_formula1race02_name'), '/default')]",
            "dependsOn": [
                "[resourceId('Microsoft.Storage/storageAccounts', parameters('storageAccounts_formula1race02_name'))]"
            ],
            "sku": {
                "name": "Standard_LRS",
                "tier": "Standard"
            },
            "properties": {
                "containerDeleteRetentionPolicy": {
                    "enabled": true,
                    "days": 7
                },
                "cors": {
                    "corsRules": []
                },
                "deleteRetentionPolicy": {
                    "allowPermanentDelete": false,
                    "enabled": true,
                    "days": 7
                }
            }
        },
        {
            "type": "Microsoft.Storage/storageAccounts/fileServices",
            "apiVersion": "2021-09-01",
            "name": "[concat(parameters('storageAccounts_formula1race02_name'), '/default')]",
            "dependsOn": [
                "[resourceId('Microsoft.Storage/storageAccounts', parameters('storageAccounts_formula1race02_name'))]"
            ],
            "sku": {
                "name": "Standard_LRS",
                "tier": "Standard"
            },
            "properties": {
                "protocolSettings": {
                    "smb": {}
                },
                "cors": {
                    "corsRules": []
                },
                "shareDeleteRetentionPolicy": {
                    "enabled": true,
                    "days": 7
                }
            }
        },
        {
            "type": "Microsoft.Storage/storageAccounts/queueServices",
            "apiVersion": "2021-09-01",
            "name": "[concat(parameters('storageAccounts_formula1race02_name'), '/default')]",
            "dependsOn": [
                "[resourceId('Microsoft.Storage/storageAccounts', parameters('storageAccounts_formula1race02_name'))]"
            ],
            "properties": {
                "cors": {
                    "corsRules": []
                }
            }
        },
        {
            "type": "Microsoft.Storage/storageAccounts/tableServices",
            "apiVersion": "2021-09-01",
            "name": "[concat(parameters('storageAccounts_formula1race02_name'), '/default')]",
            "dependsOn": [
                "[resourceId('Microsoft.Storage/storageAccounts', parameters('storageAccounts_formula1race02_name'))]"
            ],
            "properties": {
                "cors": {
                    "corsRules": []
                }
            }
        },
        {
            "type": "Microsoft.Storage/storageAccounts/blobServices/containers",
            "apiVersion": "2021-09-01",
            "name": "[concat(parameters('storageAccounts_formula1race02_name'), '/default/presentation')]",
            "dependsOn": [
                "[resourceId('Microsoft.Storage/storageAccounts/blobServices', parameters('storageAccounts_formula1race02_name'), 'default')]",
                "[resourceId('Microsoft.Storage/storageAccounts', parameters('storageAccounts_formula1race02_name'))]"
            ],
            "properties": {
                "immutableStorageWithVersioning": {
                    "enabled": false
                },
                "defaultEncryptionScope": "$account-encryption-key",
                "denyEncryptionScopeOverride": false,
                "publicAccess": "None"
            }
        },
        {
            "type": "Microsoft.Storage/storageAccounts/blobServices/containers",
            "apiVersion": "2021-09-01",
            "name": "[concat(parameters('storageAccounts_formula1race02_name'), '/default/processed')]",
            "dependsOn": [
                "[resourceId('Microsoft.Storage/storageAccounts/blobServices', parameters('storageAccounts_formula1race02_name'), 'default')]",
                "[resourceId('Microsoft.Storage/storageAccounts', parameters('storageAccounts_formula1race02_name'))]"
            ],
            "properties": {
                "immutableStorageWithVersioning": {
                    "enabled": false
                },
                "defaultEncryptionScope": "$account-encryption-key",
                "denyEncryptionScopeOverride": false,
                "publicAccess": "None"
            }
        },
        {
            "type": "Microsoft.Storage/storageAccounts/blobServices/containers",
            "apiVersion": "2021-09-01",
            "name": "[concat(parameters('storageAccounts_formula1race02_name'), '/default/raw')]",
            "dependsOn": [
                "[resourceId('Microsoft.Storage/storageAccounts/blobServices', parameters('storageAccounts_formula1race02_name'), 'default')]",
                "[resourceId('Microsoft.Storage/storageAccounts', parameters('storageAccounts_formula1race02_name'))]"
            ],
            "properties": {
                "immutableStorageWithVersioning": {
                    "enabled": false
                },
                "defaultEncryptionScope": "$account-encryption-key",
                "denyEncryptionScopeOverride": false,
                "publicAccess": "None"
            }
        }
    ]
}
