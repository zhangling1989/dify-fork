from collections.abc import Generator, Mapping
from typing import Any, Union
## zhangling code start
import logging
import core.config as config
from core.app.apps.base_app_queue_manager import AppQueueManager
import uuid
## zhangling code end

from openai._exceptions import RateLimitError

from configs import dify_config
from core.app.apps.advanced_chat.app_generator import AdvancedChatAppGenerator
from core.app.apps.agent_chat.app_generator import AgentChatAppGenerator
from core.app.apps.chat.app_generator import ChatAppGenerator
from core.app.apps.completion.app_generator import CompletionAppGenerator
from core.app.apps.workflow.app_generator import WorkflowAppGenerator
from core.app.entities.app_invoke_entities import InvokeFrom
from core.app.features.rate_limiting import RateLimit
from libs.helper import RateLimiter
from models.model import Account, App, AppMode, EndUser
from models.workflow import Workflow
from services.billing_service import BillingService
from services.errors.llm import InvokeRateLimitError
from services.workflow_service import WorkflowService


class AppGenerateService:
    system_rate_limiter = RateLimiter("app_daily_rate_limiter", dify_config.APP_DAILY_RATE_LIMIT, 86400)

    @classmethod
    def generate(
        cls,
        app_model: App,
        user: Union[Account, EndUser],
        args: Mapping[str, Any],
        invoke_from: InvokeFrom,
        streaming: bool = True,
    ):
        """
        App Content Generate
        :param app_model: app model
        :param user: user
        :param args: args
        :param invoke_from: invoke from
        :param streaming: streaming
        :return:
        """
        # system level rate limiter
        if dify_config.BILLING_ENABLED:
            # check if it's free plan
            limit_info = BillingService.get_info(app_model.tenant_id)
            if limit_info["subscription"]["plan"] == "sandbox":
                if cls.system_rate_limiter.is_rate_limited(app_model.tenant_id):
                    raise InvokeRateLimitError(
                        "Rate limit exceeded, please upgrade your plan "
                        f"or your RPD was {dify_config.APP_DAILY_RATE_LIMIT} requests/day"
                    )
                cls.system_rate_limiter.increment_rate_limit(app_model.tenant_id)

        # app level rate limiter
        max_active_request = AppGenerateService._get_max_active_requests(app_model)
        rate_limit = RateLimit(app_model.id, max_active_request)
        request_id = RateLimit.gen_request_key()
        try:
            request_id = rate_limit.enter(request_id)
            if app_model.mode == AppMode.COMPLETION.value:
                return rate_limit.generate(
                    CompletionAppGenerator.convert_to_event_stream(
                        CompletionAppGenerator().generate(
                            app_model=app_model, user=user, args=args, invoke_from=invoke_from, streaming=streaming
                        ),
                    ),
                    request_id=request_id,
                )
            elif app_model.mode == AppMode.AGENT_CHAT.value or app_model.is_agent:
                return rate_limit.generate(
                    AgentChatAppGenerator.convert_to_event_stream(
                        AgentChatAppGenerator().generate(
                            app_model=app_model, user=user, args=args, invoke_from=invoke_from, streaming=streaming
                        ),
                    ),
                    request_id,
                )
            elif app_model.mode == AppMode.CHAT.value:
                return rate_limit.generate(
                    ChatAppGenerator.convert_to_event_stream(
                        ChatAppGenerator().generate(
                            app_model=app_model, user=user, args=args, invoke_from=invoke_from, streaming=streaming
                        ),
                    ),
                    request_id=request_id,
                )
            elif app_model.mode == AppMode.ADVANCED_CHAT.value:
                workflow = cls._get_workflow(app_model, invoke_from)
                ## zhangling code start
                if "task_id" not in args and AppQueueManager.appQueueManagerGenterator == None:
                    task_id = str(uuid.uuid4())
                    AppQueueManager.task_id = task_id
                    gen = AdvancedChatAppGenerator()
                    gen.task_id = task_id
                    adGen = AdvancedChatAppGenerator.convert_to_event_stream(  # zhangling 遍历下面的生成器，本身自己也是生成器
                        gen.generate( # zhangling 是一个生成器
                            app_model=app_model,
                            workflow=workflow,
                            user=user,
                            args=args,
                            invoke_from=invoke_from,
                            streaming=streaming,
                        ),
                    )
                    logging.info(f"{config.zhangling_log_core}  添加生成器  task_id: {gen.task_id}")
                    AppQueueManager.add_appQueueManagerDict(gen.task_id + "-args", args)
                    rate_limit_generator = rate_limit.generate(
                        adGen,request_id=request_id,
                    )
                    AppQueueManager.add_appQueueManagerDict(gen.task_id + "-generator", rate_limit_generator.generator)
                    AppQueueManager.add_appQueueManagerDict(gen.task_id + "-rate_limit_generator", rate_limit_generator)
                    rate_limit_generator.task_id = gen.task_id
                    return rate_limit_generator
                else:
                    task_id = None
                    if "task_id" not in args:
                        task_id = AppQueueManager.task_id
                    else:
                        task_id = args["task_id"]
                    gen = AppQueueManager.appQueueManagerDict.pop(task_id + "-generator-copy")
                    ## zhangling 开启生成
                    logging.info(f"{config.zhangling_log_core}  重用生成器  task_id: {task_id}")
                    adGen = AdvancedChatAppGenerator.convert_to_event_stream(gen)
                    rate_limit_generator = rate_limit.generate(
                        adGen, request_id=request_id,
                    )
                    return rate_limit_generator
            ## zhangling code end
            elif app_model.mode == AppMode.WORKFLOW.value:
                workflow = cls._get_workflow(app_model, invoke_from)
                return rate_limit.generate(
                    WorkflowAppGenerator.convert_to_event_stream(
                        WorkflowAppGenerator().generate(
                            app_model=app_model,
                            workflow=workflow,
                            user=user,
                            args=args,
                            invoke_from=invoke_from,
                            streaming=streaming,
                            call_depth=0,
                            workflow_thread_pool_id=None,
                        ),
                    ),
                    request_id,
                )
            else:
                raise ValueError(f"Invalid app mode {app_model.mode}")
        except RateLimitError as e:
            raise InvokeRateLimitError(str(e))
        except Exception:
            rate_limit.exit(request_id)
            raise
        finally:
            if not streaming:
                rate_limit.exit(request_id)

    @staticmethod
    def _get_max_active_requests(app_model: App) -> int:
        max_active_requests = app_model.max_active_requests
        if max_active_requests is None:
            max_active_requests = int(dify_config.APP_MAX_ACTIVE_REQUESTS)
        return max_active_requests

    @classmethod
    def generate_single_iteration(cls, app_model: App, user: Account, node_id: str, args: Any, streaming: bool = True):
        if app_model.mode == AppMode.ADVANCED_CHAT.value:
            workflow = cls._get_workflow(app_model, InvokeFrom.DEBUGGER)
            return AdvancedChatAppGenerator.convert_to_event_stream(
                AdvancedChatAppGenerator().single_iteration_generate(
                    app_model=app_model, workflow=workflow, node_id=node_id, user=user, args=args, streaming=streaming
                )
            )
        elif app_model.mode == AppMode.WORKFLOW.value:
            workflow = cls._get_workflow(app_model, InvokeFrom.DEBUGGER)
            return AdvancedChatAppGenerator.convert_to_event_stream(
                WorkflowAppGenerator().single_iteration_generate(
                    app_model=app_model, workflow=workflow, node_id=node_id, user=user, args=args, streaming=streaming
                )
            )
        else:
            raise ValueError(f"Invalid app mode {app_model.mode}")

    @classmethod
    def generate_single_loop(cls, app_model: App, user: Account, node_id: str, args: Any, streaming: bool = True):
        if app_model.mode == AppMode.ADVANCED_CHAT.value:
            workflow = cls._get_workflow(app_model, InvokeFrom.DEBUGGER)
            return AdvancedChatAppGenerator.convert_to_event_stream(
                AdvancedChatAppGenerator().single_loop_generate(
                    app_model=app_model, workflow=workflow, node_id=node_id, user=user, args=args, streaming=streaming
                )
            )
        elif app_model.mode == AppMode.WORKFLOW.value:
            workflow = cls._get_workflow(app_model, InvokeFrom.DEBUGGER)
            return AdvancedChatAppGenerator.convert_to_event_stream(
                WorkflowAppGenerator().single_loop_generate(
                    app_model=app_model, workflow=workflow, node_id=node_id, user=user, args=args, streaming=streaming
                )
            )
        else:
            raise ValueError(f"Invalid app mode {app_model.mode}")

    @classmethod
    def generate_more_like_this(
        cls,
        app_model: App,
        user: Union[Account, EndUser],
        message_id: str,
        invoke_from: InvokeFrom,
        streaming: bool = True,
    ) -> Union[Mapping, Generator]:
        """
        Generate more like this
        :param app_model: app model
        :param user: user
        :param message_id: message id
        :param invoke_from: invoke from
        :param streaming: streaming
        :return:
        """
        return CompletionAppGenerator().generate_more_like_this(
            app_model=app_model, message_id=message_id, user=user, invoke_from=invoke_from, stream=streaming
        )

    @classmethod
    def _get_workflow(cls, app_model: App, invoke_from: InvokeFrom) -> Workflow:
        """
        Get workflow
        :param app_model: app model
        :param invoke_from: invoke from
        :return:
        """
        workflow_service = WorkflowService()
        if invoke_from == InvokeFrom.DEBUGGER:
            # fetch draft workflow by app_model
            workflow = workflow_service.get_draft_workflow(app_model=app_model)

            if not workflow:
                raise ValueError("Workflow not initialized")
        else:
            # fetch published workflow by app_model
            workflow = workflow_service.get_published_workflow(app_model=app_model)

            if not workflow:
                raise ValueError("Workflow not published")

        return workflow
