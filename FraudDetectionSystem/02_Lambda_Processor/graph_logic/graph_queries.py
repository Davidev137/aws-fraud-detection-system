# graph_logic/graph_queries.py
# Module to handle querying DynamoDB for graph-like relationships.

import boto3

# --- Configuration ---
USER_GRAPH_TABLE = "UserGraphTable"
dynamodb_client = boto3.resource('dynamodb')
table = dynamodb_client.Table(USER_GRAPH_TABLE)

def is_linked_to_fraud(node_id, node_type="user"):
    """
    Checks if a given node (like a userId, ipAddress, or deviceId)
    is connected to a known fraudulent entity in the UserGraphTable.

    Args:
        node_id (str): The ID of the node to check (e.g., "user_123").
        node_type (str): The type of the node, for logging purposes.

    Returns:
        bool: True if a connection to a fraudulent node is found, False otherwise.
    """
    print(f"Graph check for {node_type}: {node_id}")
    try:
        # This is a simplified query. A real implementation might involve
        # multi-level lookups or more complex graph patterns.
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('nodeId').eq(node_id)
        )

        items = response.get('Items', [])

        for item in items:
            # Check if any related node is marked as fraudulent
            if item.get('relatedNodeProperties', {}).get('isFraudulent', False):
                print(f"ALERT: Node {node_id} is connected to fraudulent node {item['relatedNodeId']}.")
                return True

        print(f"No direct fraudulent links found for {node_id}.")
        return False

    except Exception as e:
        print(f"Error querying UserGraphTable for node {node_id}: {e}")
        # Fail safe: if the graph check fails, assume it's not fraudulent to avoid blocking legit users.
        return False
