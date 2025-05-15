import queue
import time
from abc import abstractmethod
from enum import Enum
from typing import Any, Optional
## zhangling code start
import logging
## zhangling code end

from sqlalchemy.orm import DeclarativeMeta

from configs import dify_config
from core.app.entities.app_invoke_entities import InvokeFrom
from core.app.entities.queue_entities import (
    AppQueueEvent,
    MessageQueueMessage,
    QueueErrorEvent,
    QueuePingEvent,
    QueueStopEvent,
## zhangling code start
    QueueTextChunkEvent,
## zhangling code end
    WorkflowQueueMessage,
)
from extensions.ext_redis import redis_client


class PublishFrom(Enum):
    APPLICATION_MANAGER = 1
    TASK_PIPELINE = 2


class AppQueueManager:
    ## zhangling code start
    # 定义一个类变量 保存暂停的queue
    appQueueManagerDict = dict()
    appQueueManagerGenterator = None # test 用
    task_id = None  # test 用
    ## zhangling code end
    def __init__(self, task_id: str, user_id: str, invoke_from: InvokeFrom) -> None:
        if not user_id:
            raise ValueError("user is required")

        self._task_id = task_id
        self._user_id = user_id
        self._invoke_from = invoke_from

        user_prefix = "account" if self._invoke_from in {InvokeFrom.EXPLORE, InvokeFrom.DEBUGGER} else "end-user"
        redis_client.setex(
            AppQueueManager._generate_task_belong_cache_key(self._task_id), 1800, f"{user_prefix}-{self._user_id}"
        )

        q: queue.Queue[WorkflowQueueMessage | MessageQueueMessage | None] = queue.Queue()

        self._q = q

    def listen(self):
        """
        Listen to queue
        :return:
        """
        # wait for APP_MAX_EXECUTION_TIME seconds to stop listen
        listen_timeout = dify_config.APP_MAX_EXECUTION_TIME
        start_time = time.time()
        last_ping_time: int | float = 0
        while True:
            try:
                message = self._q.get(timeout=1)
                if message is None:
                    break

                yield message
            except queue.Empty:
                continue
            finally:
                elapsed_time = time.time() - start_time
                if elapsed_time >= listen_timeout or self._is_stopped():
                    # publish two messages to make sure the client can receive the stop signal
                    # and stop listening after the stop signal processed
                    self.publish(
                        QueueStopEvent(stopped_by=QueueStopEvent.StopBy.USER_MANUAL), PublishFrom.TASK_PIPELINE
                    )

                if elapsed_time // 10 > last_ping_time:
                    self.publish(QueuePingEvent(), PublishFrom.TASK_PIPELINE)
                    last_ping_time = elapsed_time // 10

    def stop_listen(self) -> None:
        """
        Stop listen to queue
        :return:
        """
        self._q.put(None)

    def publish_error(self, e, pub_from: PublishFrom) -> None:
        """
        Publish error
        :param e: error
        :param pub_from: publish from
        :return:
        """
        self.publish(QueueErrorEvent(error=e), pub_from)

    def publish(self, event: AppQueueEvent, pub_from: PublishFrom) -> None:
        """
        Publish event to queue
        :param event:
        :param pub_from:
        :return:
        """
        self._check_for_sqlalchemy_models(event.model_dump())
        self._publish(event, pub_from)

    @abstractmethod
    def _publish(self, event: AppQueueEvent, pub_from: PublishFrom) -> None:
        """
        Publish event to queue
        :param event:
        :param pub_from:
        :return:
        """
        raise NotImplementedError

    @classmethod
    def set_stop_flag(cls, task_id: str, invoke_from: InvokeFrom, user_id: str) -> None:
        """
        Set task stop flag
        :return:
        """
        result: Optional[Any] = redis_client.get(cls._generate_task_belong_cache_key(task_id))
        if result is None:
            return

        user_prefix = "account" if invoke_from in {InvokeFrom.EXPLORE, InvokeFrom.DEBUGGER} else "end-user"
        if result.decode("utf-8") != f"{user_prefix}-{user_id}":
            return

        stopped_cache_key = cls._generate_stopped_cache_key(task_id)
        redis_client.setex(stopped_cache_key, 600, 1)

    def _is_stopped(self) -> bool:
        """
        Check if task is stopped
        :return:
        """
        stopped_cache_key = AppQueueManager._generate_stopped_cache_key(self._task_id)
        result = redis_client.get(stopped_cache_key)
        if result is not None:
            return True

        return False

    @classmethod
    def _generate_task_belong_cache_key(cls, task_id: str) -> str:
        """
        Generate task belong cache key
        :param task_id: task id
        :return:
        """
        return f"generate_task_belong:{task_id}"

    @classmethod
    def _generate_stopped_cache_key(cls, task_id: str) -> str:
        """
        Generate stopped cache key
        :param task_id: task id
        :return:
        """
        return f"generate_task_stopped:{task_id}"

    ## zhangling code start
    @classmethod
    def set_pause_flag(cls, task_id: str, invoke_from: InvokeFrom, user_id: str) -> None:
        """
        Set task stop flag
        :return:
        """
        result: Optional[Any] = redis_client.get(cls._generate_task_belong_cache_key(task_id))
        if result is None:
            return

        user_prefix = "account" if invoke_from in {InvokeFrom.EXPLORE, InvokeFrom.DEBUGGER} else "end-user"
        if result.decode("utf-8") != f"{user_prefix}-{user_id}":
            return

        paused_cache_key = cls._generate_paused_cache_key(task_id)
        redis_client.setex(paused_cache_key, 600, 1)

    def _is_paused(self) -> bool:
        """
        Check if task is stopped
        :return:
        """
        paused_cache_key = AppQueueManager._generate_paused_cache_key(self._task_id)
        result = redis_client.get(paused_cache_key)
        if result is not None:
            return True

        return False

    @classmethod
    def is_paused(cls,task_id: str) -> bool:
        """
        Check if task is stopped
        :return:
        """
        paused_cache_key = AppQueueManager._generate_paused_cache_key(task_id)
        result = redis_client.get(paused_cache_key)
        if result is not None:
            return True

        return False

    @classmethod
    def saveMessage(cls, task_id: str,message: str):
        """
        Check if task is stopped
        :return:
        """
        generate_paused_message_cache_key = AppQueueManager._generate_paused_message_cache_key(task_id)
        result = redis_client.setex(generate_paused_message_cache_key,600,message) ## zhangling  600秒过期
        return result


    @classmethod
    def _generate_paused_cache_key(cls, task_id: str) -> str:
        """
        Generate stopped cache key
        :param task_id: task id
        :return:
        """
        return f"generate_task_paused:{task_id}"

    @classmethod
    def _generate_paused_message_cache_key(cls, task_id: str) -> str:
        """
        Generate stopped cache key
        :param task_id: task id
        :return:
        """
        return f"generate_task_paused_message:{task_id}"

    @classmethod
    def set_continue_flag(cls, task_id: str, invoke_from: InvokeFrom, user_id: str) -> None:
        """
        Set task stop flag
        :return:
        """
        result: Optional[Any] = redis_client.get(cls._generate_task_belong_cache_key(task_id))
        if result is None:
            return

        user_prefix = "account" if invoke_from in {InvokeFrom.EXPLORE, InvokeFrom.DEBUGGER} else "end-user"
        if result.decode("utf-8") != f"{user_prefix}-{user_id}":
            return

        paused_cache_key = cls._generate_paused_cache_key(task_id)

        if paused_cache_key is not None:
            generate_paused_message_cache_key = AppQueueManager._generate_paused_message_cache_key(task_id)
            text = redis_client.get(generate_paused_message_cache_key)
            logging.info(f"{text}")
            if text is not None:
                if task_id in cls.appQueueManagerDict:
                    queue = cls.appQueueManagerDict.pop(task_id)
                    event = cls.appQueueManagerDict.pop(task_id + "-event")
                    queue.publish(  ## zhangling 发送生成文本内容
                        QueueTextChunkEvent(
                            text=text,
                            from_variable_selector=event.from_variable_selector,
                            in_iteration_id=event.in_iteration_id,
                            in_loop_id=event.in_loop_id,
                        ),PublishFrom.APPLICATION_MANAGER
                    )
            redis_client.setex(paused_cache_key, 10, 0)

    @classmethod
    def add_appQueueManagerDict(cls,task_id: str,queue=None):
        cls.appQueueManagerDict.setdefault(task_id,queue)

    @classmethod
    def pop_appQueueManagerDict(cls, task_id: str):
        if task_id in cls.appQueueManagerDict:
            cls.appQueueManagerDict.pop(task_id)

    ## zhangling code end

    def _check_for_sqlalchemy_models(self, data: Any):
        # from entity to dict or list
        if isinstance(data, dict):
            for key, value in data.items():
                self._check_for_sqlalchemy_models(value)
        elif isinstance(data, list):
            for item in data:
                self._check_for_sqlalchemy_models(item)
        else:
            if isinstance(data, DeclarativeMeta) or hasattr(data, "_sa_instance_state"):
                raise TypeError(
                    "Critical Error: Passing SQLAlchemy Model instances that cause thread safety issues is not allowed."
                )


class GenerateTaskStoppedError(Exception):
    pass
