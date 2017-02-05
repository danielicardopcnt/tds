defmodule Tds.Stream do
  defstruct [:conn, :query, :params, :options, max_rows: 500]
  @type t :: %Tds.Stream{}
end
defmodule Tds.Cursor do
  defstruct [:portal, :ref, :connection_id, :max_rows]
  @type t :: %Tds.Cursor{}
end
defmodule Tds.Copy do
  defstruct [:portal, :ref, :connection_id, :query]
  @type t :: %Tds.Copy{}
end
defmodule Tds.CopyData do
  defstruct [:data, :ref]
  @type t :: %Tds.CopyData{}
end
defmodule Tds.CopyDone do
  defstruct [:ref]
  @type t :: %Tds.CopyDone{}
end

defimpl Enumerable, for: Tds.Stream do
  alias Tds.Query
  def reduce(%Tds.Stream{query: %Query{} = query} = stream, acc, fun) do
    %Tds.Stream{conn: conn, params: params, options: opts} = stream
    stream = %DBConnection.Stream{conn: conn, query: query, params: params,
                                  opts: opts}
    DBConnection.reduce(stream, acc, fun)
  end
  def reduce(%Tds.Stream{query: statement} = stream, acc, fun) do
    %Tds.Stream{conn: conn, params: params, options: opts} = stream
    query = %Query{name: "" , statement: statement}
    opts = Keyword.put(opts, :function, :prepare_open)
    stream = %DBConnection.PrepareStream{conn: conn, query: query,
                                         params: params, opts: opts}
    DBConnection.reduce(stream, acc, fun)
  end

  def member?(_, _) do
    {:error, __MODULE__}
  end

  def count(_) do
    {:error, __MODULE__}
  end
end

defimpl Collectable, for: Tds.Stream do
  alias Tds.Stream
  alias Tds.Query

  def into(%Stream{conn: %DBConnection{}} = stream) do
    %Stream{conn: conn, query: query, params: params, options: opts} = stream
    case query do
      %Query{} ->
        copy = DBConnection.execute!(conn, stream, params, opts)
        {:ok, make_into(conn, stream, copy, opts)}
      query ->
        internal = %Stream{stream | query: %Query{name: "", statement: query}}
        opts = Keyword.put(opts, :function, :prepare_into)
        {_, copy} = DBConnection.prepare_execute!(conn, internal, params, opts)
        {:ok, make_into(conn, stream, copy, opts)}
    end
  end
  def into(_) do
    msg = "data can only be copied to database inside a transaction"
    raise ArgumentError, msg
  end

  defp make_into(conn, stream, %Tds.Copy{ref: ref} = copy, opts) do
    fn
      :ok, {:cont, data} ->
        copy_data = %Tds.CopyData{ref: ref, data: data}
        _ = DBConnection.execute!(conn, copy, copy_data, opts)
        :ok
      :ok, close when close in [:done, :halt] ->
        copy_done = %Tds.CopyDone{ref: ref}
        _ = DBConnection.execute!(conn, copy, copy_done, opts)
        stream
    end
  end
end

defimpl DBConnection.Query, for: Tds.Stream do
  alias Tds.Stream

  def parse(%Stream{query: query} = stream, opts) do
    %Stream{stream | query: DBConnection.Query.parse(query, opts)}
  end

  def describe(%Stream{query: query} = stream, opts) do
    %Stream{stream | query: DBConnection.Query.describe(query, opts)}
  end

  def encode(%Stream{query: query}, params, opts) do
    DBConnection.Query.encode(query, params, opts)
  end

  def decode(_, copy, _), do: copy
end

defimpl DBConnection.Query, for: Tds.Copy do
  alias Tds.Copy
  import Tds.Messages

  def parse(copy, _) do
    raise "can not prepare #{inspect copy}"
  end

  def describe(copy, _) do
    raise "can not describe #{inspect copy}"
  end

  def encode(%Copy{ref: ref}, %Tds.CopyData{data: data, ref: ref}, _) do
    try do
      encode_msg(msg_copy_data(data: data))
    rescue
      ArgumentError ->
        raise ArgumentError,
          "expected iodata to copy to database, got: " <> inspect(data)
    else
      iodata ->
        {:copy_data, iodata}
    end
  end

  def encode(%Copy{ref: ref}, %Tds.CopyDone{ref: ref}, _) do
    :copy_done
  end

  def decode(%Copy{query: query}, result, opts) do
    case result do
      %Tds.Result{command: :copy_stream} ->
        result
      %Tds.Result{command: :close} ->
        result
      _ ->
        DBConnection.Query.decode(query, result, opts)
    end
  end
end

defimpl String.Chars, for: Tds.Stream do
  def to_string(%Tds.Stream{query: query}) do
    String.Chars.to_string(query)
  end
end

defimpl String.Chars, for: Tds.Copy do
  def to_string(%Tds.Copy{query: query}) do
    String.Chars.to_string(query)
  end
end
