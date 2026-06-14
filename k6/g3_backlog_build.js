import http from "k6/http";
import { check, sleep } from "k6";
import { Counter } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const EVENT_ID = __ENV.EVENT_ID || "EVT-001";

export const timeout_errors = new Counter("timeout_errors");
export const status_0_errors = new Counter("status_0_errors");
export const status_429_errors = new Counter("status_429_errors");
export const status_5xx_errors = new Counter("status_5xx_errors");

export const options = {
    discardResponseBodies: true,

    scenarios: {
        g3_backlog_build: {
            executor: "constant-arrival-rate",
            rate: Number(__ENV.RATE || 30),     // 로컬: 30 rps (AWS: 80 rps)
            timeUnit: "1s",
            duration: __ENV.DURATION || "3m",   // 로컬: 3분 (AWS: 5분)
            preAllocatedVUs: 150,
            maxVUs: 500,
        },
    },

    thresholds: {
        http_req_failed: ["rate<0.01"],
        http_req_duration: ["p(95)<3000"],
        timeout_errors: ["count<50"],
        status_0_errors: ["count<50"],
        status_429_errors: ["count<1"],
        status_5xx_errors: ["count<1"],
    },
};

export default function () {
    const userId = `k6-user-${__VU}-${__ITER}-${Date.now()}`;

    const res = http.post(
        `${BASE_URL}/api/events/${EVENT_ID}/apply?eventType=FIRST_COME`,
        null,
        {
            timeout: "15s",
            headers: { "X-User-Id": userId },
        }
    );

    const ok = check(res, {
        "202 accepted": (r) => r && r.status === 202,
    });

    if (!ok) {
        const d = res && res.timings ? res.timings.duration : 0;

        if (!res || res.status === 0) {
            status_0_errors.add(1);
            if (d >= 14900) timeout_errors.add(1);
        } else if (res.status === 429) {
            status_429_errors.add(1);
        } else if (res.status >= 500) {
            status_5xx_errors.add(1);
        }
    }

    sleep(0.01);
}
