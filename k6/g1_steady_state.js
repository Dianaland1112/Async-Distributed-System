import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const EVENT_ID = __ENV.EVENT_ID || "EVT-001";
const VUS = Number(__ENV.VUS || 100);      // 로컬 부하테스트: 100 (AWS: 2000)
const DURATION = __ENV.DURATION || "5m";

export const options = {
    scenarios: {
        steady: {
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
    const userId = `user-steady-${__VU}-${Math.random().toString(36).substring(2, 10)}`;

    const res = http.post(
        `${BASE_URL}/api/events/${EVENT_ID}/apply?eventType=FIRST_COME`,
        null,
        {
            headers: { "X-User-Id": userId },
        }
    );

    check(res, {
        "status is 202": (r) => r.status === 202,
    });

    sleep(0.1);
}
