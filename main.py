import redis
import uuid
from datetime import datetime, timezone

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Lua script for atomic voucher redemption using hashes
lua_script = """
local current = redis.call('HGET', KEYS[1], ARGV[1])
if current and tonumber(current) > 0 then
    return redis.call('HINCRBY', KEYS[1], ARGV[1], -1)
else
    return -1
end
"""

# Register the Lua script with Redis
redeem_voucher_script = r.register_script(lua_script)

def generate_voucher(uses, metadata=None):
    """
    Generates a unique voucher code with a specified number of uses and metadata.

    Args:
        uses (int): The number of times the voucher can be redeemed.
        metadata (dict, optional): Additional voucher metadata.

    Returns:
        str: The unique voucher code.
    """
    # Generate a unique voucher code
    voucher_code = str(uuid.uuid4())

    # Prepare the voucher data
    voucher_data = {
        'uses': uses,
        'created_at': datetime.now(timezone.utc).isoformat()
    }
    if metadata:
        voucher_data.update(metadata)

    # Store the voucher data as a hash in Redis
    r.hset(voucher_code, mapping=voucher_data)

    print(f"Voucher '{voucher_code}' generated with {uses} uses.")
    return voucher_code

def redeem_voucher(voucher_code):
    """
    Attempts to redeem a voucher code atomically.

    Args:
        voucher_code (str): The voucher code to redeem.

    Returns:
        int: Remaining uses after redemption, or -1 if redemption failed.
    """
    # Execute the Lua script atomically
    result = redeem_voucher_script(keys=[voucher_code], args=['uses'])

    if int(result) >= 0:
        print(f"Voucher '{voucher_code}' redeemed successfully. Remaining uses: {result}.")
    else:
        print(f"Voucher '{voucher_code}' has been fully redeemed or does not exist.")

    return int(result)

def get_voucher_info(voucher_code):
    """
    Retrieves voucher metadata.

    Args:
        voucher_code (str): The voucher code.

    Returns:
        dict: Voucher metadata.
    """
    voucher_data = r.hgetall(voucher_code)
    # Decode bytes to strings
    voucher_info = {key.decode('utf-8'): value.decode('utf-8') for key, value in voucher_data.items()}
    return voucher_info

def main():
    # Generate a voucher with a given number of uses and additional metadata
    metadata = {
        'description': '10% off on all items',
        'expiry_date': '2024-12-31T23:59:59'
    }
    voucher_code = generate_voucher(uses=4, metadata=metadata)

    # Retrieve and display voucher metadata
    print("\nVoucher Metadata:")
    voucher_info = get_voucher_info(voucher_code)
    for key, value in voucher_info.items():
        print(f"{key}: {value}")

    # Simulate multiple users attempting to redeem the voucher
    for i in range(1, 6):
        print(f"\nAttempt {i} to redeem voucher:")
        redeem_voucher(voucher_code)

if __name__ == "__main__":
    main()