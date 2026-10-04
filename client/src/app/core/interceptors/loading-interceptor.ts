import { HttpInterceptorFn } from '@angular/common/http';
import { finalize } from 'rxjs/internal/operators/finalize';
import { environment } from '../../../environments/environment';
import { delay, identity } from 'rxjs';
import { BusyService } from '../services/busy.service';
import { inject } from '@angular/core';

export const loadingInterceptor: HttpInterceptorFn = (req, next) => {
  const busyService = inject(BusyService);
  
  busyService.busy();
  
  return next(req).pipe(
    (environment.production ? identity : delay(500)),
    finalize(() => busyService.idle())
  )
};
