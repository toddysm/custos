# Activities API Reference: <Project Name>

Last Updated: YYYY-MM-DD

## Overview

<1-2 sentence description of what an activity is and what it enables>

## Activity Contract

Every activity must implement the following interface:

```typescript
interface Activity {
  id: string
  name: string
  description: string
  inputSchema: JSONSchema
  outputSchema: JSONSchema
  execute(input: ActivityInput, context: ActivityContext): Promise<ActivityOutput>
}
```

## Input & Output

| Field | Type | Description |
|---|---|---|
| `input` | Defined by `inputSchema` | Data passed into the activity |
| `output` | Defined by `outputSchema` | Data returned by the activity |
| `context` | `ActivityContext` | Runtime context: auth tokens, logger, timeout |

## Activity Context

```typescript
interface ActivityContext {
  auth: AuthToken         // caller's auth context
  logger: Logger          // structured logger
  timeout: number         // milliseconds until hard timeout
  signal: AbortSignal     // cancellation signal
}
```

## Progress Reporting

<How an activity reports intermediate progress to the host>

## Error Handling

| Error Type | Cause | Retry Behavior |
|---|---|---|
| `ValidationError` | Input failed schema validation | No retry |
| `TimeoutError` | Activity exceeded timeout | Configurable retry |
| `ActivityError` | Business logic failure | No retry by default |

## Composition & Chaining

<Describe if and how activities can be composed or chained>

## Change History

| Date | Change |
|---|---|
| YYYY-MM-DD | Initial |
