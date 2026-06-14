import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const EVENT_ID = __ENV.EVENT_ID || "EVT-001";

export const options = {
    discardResponseBodies: true,

    scenarios: {
        ramp_test: {
            executor: "ramping-arrival-rate",
            timeUnit: "1s",
            stages: [
                { target: 10, duration: "1m" },   // 안정 구간
                { target: 20, duration: "1m" },
                { target: 40, duration: "1m" },
                { target: 80, duration: "1m" },
                { target: 100, duration: "1m" },  // 한계 탐색 (로컬: 100, AWS: 300)
            ],
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
    const userId = `k6-user-${__VU}-${__ITER}`;

    const res = http.post(
        `${BASE_URL}/api/events/${EVENT_ID}/apply?eventType=FIRST_COME`,
        null,
        {
            timeout: "20s",
            headers: { "X-User-Id": userId },
        }
    );

    check(res, {
        "202 accepted": (r) => r.status === 202,
    });

    sleep(0.01);
}
