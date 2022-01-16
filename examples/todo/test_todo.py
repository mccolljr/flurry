import asyncio


async def test_main():
    import os
    import atexit
    import aiohttp

    todo_app_path = os.path.join(os.path.dirname(__file__), "__main__.py")
    app_proc = await asyncio.create_subprocess_shell(f"python {todo_app_path}")
    atexit.register(lambda **_: app_proc.kill())

    await asyncio.sleep(1)

    test_todo = {
        "title": "Test Todo",
        "description": "Test that the Todo graphql endpoints work",
    }

    async with aiohttp.client.request(
        "POST",
        "http://localhost:8080",
        json={
            "query": r"""
                mutation($title: String!, $description: String!) {
                    CreateTodoCommand(title: $title, description: $description) {
                        ok
                    }
                }
            """,
            "variables": test_todo,
        },
    ) as response:
        assert (
            response.status == 200
        ), f"wanted 200, got {response.status}: {await response.text()}"
        assert await response.json() == {"CreateTodoCommand": {"ok": True}}

    async with aiohttp.client.request(
        "POST",
        "http://localhost:8080",
        json={
            "query": r"""
                query {
                    ListTodosQuery {
                        todos {
                            todoId
                            title
                            description
                            createdAt
                            updatedAt
                            completedAt
                        }
                    }
                }
            """,
            "variables": {},
        },
    ) as response:
        assert (
            response.status == 200
        ), f"wanted 200, got {response.status}: {await response.text()}"
        result = await response.json()
        assert len(result["ListTodosQuery"]["todos"]) > 0
        assert result["ListTodosQuery"]["todos"][0]["title"] == test_todo["title"]
        assert (
            result["ListTodosQuery"]["todos"][0]["description"]
            == test_todo["description"]
        )
        assert result["ListTodosQuery"]["todos"][0]["completedAt"] is None
        test_todo["todoId"] = result["ListTodosQuery"]["todos"][0]["todoId"]

    async with aiohttp.client.request(
        "POST",
        "http://localhost:8080",
        json={
            "query": r"""
                mutation($todoId: String!) {
                    CompleteTodoCommand(todoId: $todoId) {
                        ok
                    }
                }
            """,
            "variables": {"todoId": test_todo["todoId"]},
        },
    ) as response:
        assert (
            response.status == 200
        ), f"wanted 200, got {response.status}: {await response.text()}"
        assert await response.json() == {"CompleteTodoCommand": {"ok": True}}

    async with aiohttp.client.request(
        "POST",
        "http://localhost:8080",
        json={
            "query": r"""
                query {
                    ListTodosQuery {
                        todos {
                            todoId
                            title
                            description
                            createdAt
                            updatedAt
                            completedAt
                        }
                    }
                }
            """,
            "variables": {},
        },
    ) as response:
        assert (
            response.status == 200
        ), f"wanted 200, got {response.status}: {await response.text()}"
        result = await response.json()
        assert len(result["ListTodosQuery"]["todos"]) > 0
        assert result["ListTodosQuery"]["todos"][0]["title"] == test_todo["title"]
        assert (
            result["ListTodosQuery"]["todos"][0]["description"]
            == test_todo["description"]
        )
        assert result["ListTodosQuery"]["todos"][0]["todoId"] == test_todo["todoId"]
        assert result["ListTodosQuery"]["todos"][0]["completedAt"] is not None

    async with aiohttp.client.request(
        "POST",
        "http://localhost:8080",
        json={
            "query": r"""
                query($todoId: String!) {
                    FindTodoQuery(todoId: $todoId) {
                        found {
                            todoId
                            title
                            description
                            createdAt
                            updatedAt
                            completedAt
                        }
                    }
                }
            """,
            "variables": {"todoId": test_todo["todoId"]},
        },
    ) as response:
        assert (
            response.status == 200
        ), f"wanted 200, got {response.status}: {await response.text()}"
        result = await response.json()
        assert result["FindTodoQuery"]["found"] is not None
        assert result["FindTodoQuery"]["found"]["todoId"] == test_todo["todoId"]
        assert result["FindTodoQuery"]["found"]["title"] == test_todo["title"]
        assert (
            result["FindTodoQuery"]["found"]["description"] == test_todo["description"]
        )


if __name__ == "__main__":
    asyncio.run(test_main())
