export const isPresent = <T>(obj: T): obj is NonNullable<T> => obj !== null && obj !== undefined;
