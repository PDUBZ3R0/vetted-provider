import type { Report } from './probes.ts';
export declare const PROFILES: readonly ["scanner", "trader", "reader", "subscriber"];
export type Profile = typeof PROFILES[number];
export type Rating = {
    pass: boolean;
    score: number;
    why?: string;
};
export type GateOpts = {
    maxLag: number;
};
export declare function rate(r: Report, p: Profile, o: GateOpts): Rating;
export declare function rateAll(r: Report, o: GateOpts): Record<Profile, Rating>;
