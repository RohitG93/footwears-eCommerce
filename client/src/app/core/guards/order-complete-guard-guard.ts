import { inject } from '@angular/core/primitives/di';
import { CanActivateFn, Router } from '@angular/router';
import { OrderService } from '../services/order.service';

export const orderCompleteGuardGuard: CanActivateFn = (route, state) => {
  const orderService = inject(OrderService);
  const router = inject(Router);

  if (orderService.orderComplete) {
    return true;
  } else {
    router.navigateByUrl('/shop');
    return false;
  }
};
