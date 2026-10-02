import { inject, Service } from '@angular/core';
import { CartService } from './cart.service';
import { forkJoin, of, tap } from 'rxjs';
import { AccountService } from './account.service';
import { SignalrService } from './signalr.service';

@Service()
export class InitService {
    private cartService = inject(CartService)
    private accService = inject(AccountService)
    private signalrService = inject(SignalrService);
;
    init() {
        const cart_id = localStorage.getItem('cart_id');
        const carts = cart_id ?  this.cartService.getCart(cart_id) : of(null);

        return forkJoin(
            {
                cart: carts,
                userInfo: this.accService.getUserInfo().pipe(
                    tap(user => {
                        if (user) this.signalrService.createHubConnection()
                        })
                )
            }
        )
    }
}
