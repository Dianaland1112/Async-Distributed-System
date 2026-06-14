import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const EVENT_ID = __ENV.EVENT_ID || "EVT-001";

export const options = {
    discardResponseBodies: true,

    scenarios: {
        slo: {
            executor: "constant-arrival-rate",
            rate: 30,
            timeUnit: "1s",
            duration: "5m",
            preAllocatedVUs: 100,
            maxVUs: 300,
        },
    },

    thresholds: {
        http_req_failed: ["rate<0.01"],
        http_req_duration: ["p(95)<3000"],
    },
};

export default function () {
    const userId = `user-${__VU}-${__ITER}-${Date.now()}`;

    const res = http.post(
        `${BASE_URL}/api/events/${EVENT_ID}/apply?eventType=FIRST_COME`,
        null,
        {
            headers: { "X-User-Id": userId },
            timeout: "15s",
        }
    );

    check(res, {
        "202 accepted": (r) => r.status === 202,
    });

    sleep(0.01);
}
