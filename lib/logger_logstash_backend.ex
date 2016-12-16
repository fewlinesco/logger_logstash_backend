################################################################################
# Copyright 2015 Marcelo Gornstein <marcelog@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
defmodule LoggerLogstashBackend do
  use GenEvent
  use Timex

  def init({__MODULE__, name}) do
    {:ok, configure(name, [])}
  end

  def handle_call({:configure, opts}, %{name: name}) do
    {:ok, :ok, configure(name, opts)}
  end

  def handle_event(:flush, state) do
    {:ok, state}
  end

  def handle_event(
    {level, _gl, {Logger, msg, ts, md}}, %{level: min_level} = state
  ) do
    if is_nil(min_level) or Logger.compare_levels(level, min_level) != :lt do
      log_event level, msg, ts, md, state
    end
    {:ok, state}
  end

  defp log_event(
    level, msg, ts, md, %{
      host: host,
      port: port,
      type: type,
      metadata: metadata,
      socket: socket
    }
  ) do
    fields = md
             |> Keyword.merge(metadata)
             |> Enum.into(%{})
             |> Map.put(:level, to_string(level))
             |> inspect_pids

    {{year, month, day}, {hour, minute, second, milliseconds}} = ts
    {:ok, ts} = NaiveDateTime.new(
      year, month, day, hour, minute, second, (milliseconds * 1000)
    )
    ts = Timex.to_datetime ts, Timezone.local
    {:ok, json} = JSX.encode %{
      type: type,
      "@timestamp": Timex.format!(ts, "%FT%T.%f%z", :strftime),
      message: to_string(msg),
      fields: fields
    }
    :gen_udp.send socket, host, port, to_char_list(json)
  end

  defp configure(name, opts) do
    env = Application.get_env :logger, name, []
    opts = Keyword.merge env, opts
    Application.put_env :logger, name, opts

    level = fetch_configuration_value(opts, :level, :debug)
    metadata = fetch_configuration_value(opts, :metadata, [])
    type = fetch_configuration_value(opts, :type, "elixir")
    host = fetch_configuration_value(opts, :host)
    port = to_i!(fetch_configuration_value(opts, :port), "port has to be an integer")
    {:ok, socket} = :gen_udp.open 0

    %{
      name: name,
      host: to_char_list(host),
      port: port,
      level: level,
      socket: socket,
      type: type,
      metadata: metadata
    }
  end

  defp fetch_configuration_value(opts, name, default) do
    fetch_configuration_value(opts, name) || default
  end

  defp fetch_configuration_value(opts ,name) do
    case Keyword.get(opts, name) do
      {:system, name} ->
        System.get_env(name)
      value ->
        value
    end
  end

  # inspects the argument only if it is a pid
  defp inspect_pid(pid) when is_pid(pid), do: inspect(pid)
  defp inspect_pid(other), do: other

  # inspects the field values only if they are pids
  defp inspect_pids(fields) when is_map(fields) do
    Enum.into fields, %{}, fn {key, value} ->
      {key, inspect_pid(value)}
    end
  end

  defp to_i!(value, error_message) do
    case Integer.parse(value) do
      :error ->
        raise error_message
      {int, _rest} ->
        int
    end
  end
end
