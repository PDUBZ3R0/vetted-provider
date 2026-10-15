/** Reverts with the 4 bytes 0xdeadbeef. Does the RPC hand the revert data back? */
export declare const REVERT_DEADBEEF: string;
export declare const REVERT_MARKER = "deadbeef";
/** Returns GAS (gas left) as a 32-byte word: shows the gas the node actually granted the call. */
export declare const RETURN_GAS = "0x5a60005260206000f3";
/** Runtime code returning the word 42 — installed with a state override. */
export declare const RETURN_42 = "0x602a60005260206000f3";
/** keccak256("Transfer(address,address,uint256)") — the most common topic on any EVM chain. */
export declare const TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
/** keccak256("Approval(address,address,uint256)") */
export declare const APPROVAL_TOPIC = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925";
/** A random address: nothing lives there, so a getLogs filter on it returns no rows — it measures the
 *  RANGE limit alone, without a result-count limit getting in the way. */
export declare function randomAddress(): string;
