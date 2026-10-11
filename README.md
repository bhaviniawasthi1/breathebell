# BreatheBell

BreatheBell tells a school, every morning, whether its children can play outside today. It gives one clear answer for each class group (Allowed, Limited or Indoor only), the cleanest outdoor slot of the day, and a one-page circular that can be printed for parents.

## The problem

Air pollution in many Indian cities changes from hour to hour. Schools usually have no simple way to decide if PE, sports or assembly should happen outdoors. Raw AQI numbers are hard for a principal to act on, and the same air is not equally safe for a six-year-old and a seventeen-year-old.

## What it does

- **Class-wise decision.** Primary, middle and senior students, plus children with asthma or allergies, each get their own decision based on PM2.5.
- **Best outdoor slot.** Finds the cleanest 2-hour slot inside school hours (8 to 4) for today and the next two days.
- **Bell schedule and 48-hour chart.** Shows the decision for every school hour and a forecast chart.
- **District view.** Ranks schools by air quality so an officer can see which schools need attention first.
- **Alerts.** An hourly check sends an email when a school moves into or out of "Indoor only". A morning digest at 07:00 IST lists every school.
- **Printable circular.** A one-page summary for parents, saved as PDF from the browser print dialog.
- **Impact estimate.** Student-hours of outdoor exposure avoided over the next 48 school hours if the advice is followed.

## How it works

```
Browser (static site)
        |
        v
API Gateway (HTTP API) --> Lambda (api_handler) --> DynamoDB (schools, history)
                                   |
                                   v
                            Open-Meteo APIs (air quality + geocoding)

EventBridge (every hour)   --> Lambda (scheduled_handler) --> SNS email alert
EventBridge (07:00 IST)    --> Lambda (digest_handler)    --> SNS morning digest
```

AWS services: Lambda, API Gateway, DynamoDB, EventBridge, SNS. The whole stack is defined in `template.yaml` and deployed with AWS SAM.

## Decision rules

PM2.5 limits in ug/m3. Younger and sensitive children get stricter limits.

| Group | Allowed up to | Limited up to | Above |
|---|---|---|---|
| Primary (Class 1-5) | 30 | 60 | Indoor only |
| Middle (Class 6-8) | 45 | 75 | Indoor only |
| Senior (Class 9-12) | 60 | 90 | Indoor only |
| Asthma or allergy | 15 | 30 | Indoor only |

The school-wide decision follows the most restrictive class group present in the school. Limits are adapted from CPCB and WHO PM2.5 guidance. They are planning thresholds for schools, not medical advice.

## API

| Method | Path | Purpose |
|---|---|---|
| POST | `/schools` | Add a school (name, city or lat/lon, student counts) |
| GET | `/schools` | List schools |
| GET | `/schools/{id}/status` | Current decision, class-wise results, best slot, 48-hour forecast, impact |
| GET | `/schools/{id}/history` | Recent readings |
| GET | `/overview` | District view with totals |

## Data and limits

- Air quality comes from the Open-Meteo Air Quality API. It is a modelled estimate for the school's location, not a reading from a sensor at the school gate.
- The impact number is an estimate. Indoor hours count in full and Limited hours count as half.
- Alerts currently go to one email address configured at deploy time.

## Deploy

Requirements: AWS account, AWS CLI and AWS SAM CLI.

```
sam build
sam deploy --guided
```

Then set `API` in `frontend/index.html` to the `ApiUrl` printed in the stack outputs, and host the `frontend` folder on any static host (for example AWS Amplify).

## Project structure

```
template.yaml        AWS SAM template (all AWS resources)
src/app.py           Lambda code: decision engine, forecast, alerts, API
frontend/index.html  Single-page dashboard
```

## Author

Built by Bhavini Awasthi.
