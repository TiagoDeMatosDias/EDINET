# Orchestration Rework


The current orchestration layer is a mess with a lot of duplicated code and a lot of code that is not used anymore, and weird choices and dependencies (why is the edinet a class that needs to be initiated? Why not just use functions? Why is the configuration tightly coupled with the class? Why are there so many nested function calls?). 

The goal of this rework is to clean up the orchestration layer and make it more maintainable.

A few things that need to be changed:

- Every underlying component (edinet, database, etc.) should be a separate module with a clear interface. The orchestration layer should just call these modules and not have any logic of its own.
- The underlying components should be stateless and should not have any side effects. They should just take input and return output. No global state or class variables should be used.
- The orchestration layer should be a simple function that takes input and returns output. It should not have any logic of its own and should just call the underlying components in the correct order.
- The configuration should be passed as an argument to the orchestration function and should not be tightly coupled with the underlying components. The underlying components should just take the configuration as an argument and not have any knowledge of where it came from.
- The orchestration layer should be easy to test and should not have any dependencies on external services or databases. The underlying components should be easy to mock and should not have any side effects that could affect the tests.
- The orchestration layer should be easy to extend and should not have any hardcoded logic. The underlying components should be easy to replace and should not have any dependencies on each other.
- The orchestration layer should be easy to read and understand. The underlying components should have clear and concise interfaces and should not have any unnecessary complexity.
- The orchestration layer should be easy to debug and should not have any hidden state or side effects. The underlying components should be easy to debug and should not have any hidden state or side effects.
