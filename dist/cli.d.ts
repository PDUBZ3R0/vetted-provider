#!/usr/bin/env -S node
import { type Report } from './probes.ts';
import { type Profile, type Rating } from './score.ts';
export type Results = {
    tool: string;
    chainId: number;
    name: string;
    testedAt: string;
    from: string;
    options: Record<string, unknown>;
    endpoints: Array<Report & {
        ratings: Record<Profile, Rating>;
    }>;
};
