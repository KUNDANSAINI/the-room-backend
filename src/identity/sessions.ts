import type { DailyRoom } from "../room/day.js";
import type { SessionRecord, StateStore } from "../store/types.js";
import { generatePublicName } from "./names.js";
import { hashToken, newResumeToken, newSessionId } from "./tokens.js";

export interface NewSessionInput {
  ipHash: string;
  country: string | null;
  now: number;
  /** Abuse state carried in memory across midnight for a live connection (never persisted as a link). */
  carry?: Pick<SessionRecord, "suspicion" | "challengePending">;
}

/**
 * Anonymous, per-day sessions. Everything a session owns (record, public
 * name, resume token) expires exactly at the room's end — there is no
 * permanent identity and no link between days.
 */
export class SessionService {
  constructor(private readonly store: StateStore) {}

  async create(room: DailyRoom, input: NewSessionInput): Promise<{ rec: SessionRecord; token: string }> {
    const sid = newSessionId();
    let name: string | null = null;
    for (let i = 0; i < 10 && !name; i++) {
      const candidate = generatePublicName();
      if (await this.store.claimName(room.roomId, candidate, sid, room.endsAt)) name = candidate;
    }
    if (!name) throw new Error("could not allocate a unique public name");

    const rec: SessionRecord = {
      sid,
      roomId: room.roomId,
      name,
      country: input.country,
      ipHash: input.ipHash,
      createdAt: input.now,
      suspicion: input.carry?.suspicion ?? 0,
      suspicionAt: input.now,
      challengePending: input.carry?.challengePending ?? false,
      challengeFails: 0,
      trustedUntil: 0,
      blocks: 0,
    };
    const token = newResumeToken();
    await Promise.all([
      this.store.saveSession(rec, room.endsAt),
      this.store.setResume(hashToken(token), sid, room.endsAt),
    ]);
    return { rec, token };
  }

  /** Returns the session for a resume token only if it belongs to the current room. */
  async resume(room: DailyRoom, token: string): Promise<SessionRecord | null> {
    if (!token || token.length > 128) return null;
    const sid = await this.store.resolveResume(hashToken(token));
    if (!sid) return null;
    const rec = await this.store.getSession(sid);
    if (!rec || rec.roomId !== room.roomId) return null;
    return rec;
  }

  save(rec: SessionRecord, room: DailyRoom) {
    return this.store.saveSession(rec, room.endsAt);
  }
}
