// -----------------------------------------------------------------------------
// Tiny EVM programs for eth_call probes. Each runs as INIT CODE (eth_call with
// no `to`) or as overridden code, so no contract has to exist on the chain.
// -----------------------------------------------------------------------------

/** Reverts with the 4 bytes 0xdeadbeef. Does the RPC hand the revert data back? */
//   PUSH4 deadbeef  PUSH1 0  MSTORE  PUSH1 4  PUSH1 28  REVERT
export const REVERT_DEADBEEF = '0x63deadbeef600052600460' + '1cfd';
export const REVERT_MARKER = 'deadbeef';

/** Returns GAS (gas left) as a 32-byte word: shows the gas the node actually granted the call. */
//   GAS  PUSH1 0  MSTORE  PUSH1 32  PUSH1 0  RETURN
export const RETURN_GAS = '0x5a60005260206000f3';

/** Runtime code returning the word 42 — installed with a state override. */
//   PUSH1 42  PUSH1 0  MSTORE  PUSH1 32  PUSH1 0  RETURN
export const RETURN_42 = '0x602a60005260206000f3';

/** keccak256("Transfer(address,address,uint256)") — the most common topic on any EVM chain. */
export const TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
/** keccak256("Approval(address,address,uint256)") */
export const APPROVAL_TOPIC = '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925';

/** A random address: nothing lives there, so a getLogs filter on it returns no rows — it measures the
 *  RANGE limit alone, without a result-count limit getting in the way. */
export function randomAddress(): string {
    const b = new Uint8Array(20);
    crypto.getRandomValues(b);
    return '0x' + Buffer.from(b).toString('hex');
}
