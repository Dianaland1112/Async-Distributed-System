import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const EVENT_ID = __ENV.EVENT_ID || "EVT-001";

export const options = {
    discardResponseBodies: false,

    scenarios: {
        slo_measurement: {
            executor: "constant-arrival-rate",
            rate: 30,           // 초당 30 req
            timeUnit: "1s",
            duration: "5m",
            preAllocatedVUs: 100,
            maxVUs: 300,        // 로컬: 300 (AWS: 800)
        },
    },

    thresholds: {
        http_req_failed: ["rate<0.01"],
        http_req_duration: ["p(95)<3000"],  // p95 < 3s
    },
};

export default function () {
    const userId = `k6-user-${__VU}-${__ITER}`;

    const res = http.post(
        `${BASE_URL}/api/events/${EVENT_ID}/apply?eventType=FIRST_COME`,
        null,
        {
            timeout: "15s",
            headers: { "X-User-Id": userId },
        }
    );

    check(res, {
        "202 accepted": (r) => r.status === 202,
        "body has requestId": (r) => r.body && r.body.includes("requestId"),
    });

    sleep(0.01);
}
