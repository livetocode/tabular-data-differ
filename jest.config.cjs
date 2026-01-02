/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  collectCoverage: true,
  roots: ['<rootDir>/src'],
  coverageDirectory: './output/coverage',
  coverageThreshold: {
    global: {
      branches: 97,
      functions: 100,
      lines: 100,
      statements: 100,
    },
  },
};