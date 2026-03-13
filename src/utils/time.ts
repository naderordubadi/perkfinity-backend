export function addMinutes(date: Date, minutes: number): Date {
  return new Date(date.getTime() + minutes * 60000);
}

export function isExpired(expiresAt: Date): boolean {
  return new Date() > expiresAt;
}

export function nowPlusMinutes(minutes: number): Date {
  return addMinutes(new Date(), minutes);
}
