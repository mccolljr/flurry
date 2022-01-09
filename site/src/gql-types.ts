export type Maybe<T> = T | null;
export type InputMaybe<T> = T | null;
export type Exact<T extends { [key: string]: unknown }> = { [K in keyof T]: T[K] };
export type MakeOptional<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]?: Maybe<T[SubKey]> };
export type MakeMaybe<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]: Maybe<T[SubKey]> };
/** All built-in and custom scalars, mapped to their actual values */
export interface Scalars {
  ID: string;
  String: string;
  Boolean: boolean;
  Int: number;
  Float: number;
  /**
   * The `DateTime` scalar type represents a DateTime
   * value as specified by
   * [iso8601](https://en.wikipedia.org/wiki/ISO_8601).
   */
  DateTime: string;
}

export interface ArgumentsInput {
  readonly username: Scalars['String'];
}

export type LoadUserQueryVariables = Exact<{
  username: Scalars['String'];
}>;


export type LoadUserQuery = { readonly FindUserByUsername: { readonly user: { readonly email: string, readonly username: string, readonly lastLogin: string | null } | null } | null };

export type RegisterUserMutationVariables = Exact<{
  email: Scalars['String'];
  lastName: Scalars['String'];
  firstName: Scalars['String'];
  username: Scalars['String'];
  password: Scalars['String'];
}>;


export type RegisterUserMutation = { readonly CreateUserCommand: { readonly ok: boolean } | null };
