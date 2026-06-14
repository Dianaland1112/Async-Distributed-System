import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const EVENT_ID = __ENV.EVENT_ID || "EVT-001";
const VUS = Number(__ENV.VUS || 50);
const DURATION = __ENV.DURATION || "60s";

export const options = {
    scenarios: {
        baseline: {
            executor: "constant-vus",
            vus: VUS,
            duration: DURATION,
        },
    },
    thresholds: {
        http_req_failed: ["rate<0.01"],
    },
};

export default function () {
    const userId = `user-g2-${__VU}-${Math.random().toString(36).substring(2, 8)}`;

    const res = http.post(
        `${BASE_URL}/api/events/${EVENT_ID}/apply?eventType=FIRST_COME`,
        null,
        {
            headers: { "X-User-Id": userId },
        }
    );

    check(res, {
        "202 accepted": (r) => r.status === 202,
        "has requestId": (r) => r.body && r.body.includes("requestId"),
    });

    sleep(0.2);
}
